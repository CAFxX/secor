package com.pinterest.secor.uploader;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.ThreadPoolUtil;

/**
 * Manages uploads to Microsoft Azure blob storage using Azure Storage SDK for
 * java https://github.com/azure/azure-storage-java
 *
 * @author Taichi Nakashima (nsd22843@gmail.com)
 * @author Carlo Alberto Ferraris (cafxx@strayorange.com)
 *
 */
public class AzureUploadManager extends UploadManager {
	private static final Logger LOG = LoggerFactory.getLogger(AzureUploadManager.class);
	private static final ExecutorService executor = ThreadPoolUtil.createCachedThreadPool(256, "AzureUploadManager");

	private CloudBlobClient blobClient;
	private final int uploadTimeout;
	private final String azureContainer;
	private final String azurePath;

	public AzureUploadManager(SecorConfig config) throws Exception {
		super(config);

		final String storageConnectionString = 
				"DefaultEndpointsProtocol=" + mConfig.getAzureEndpointsProtocol() + ";" +
				"AccountName=" + mConfig.getAzureAccountName() + ";" + 
				"AccountKey=" + mConfig.getAzureAccountKey() + ";";

		CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
		blobClient = storageAccount.createCloudBlobClient();

		uploadTimeout = mConfig.getAzureUploadTimeout();
		azureContainer = mConfig.getAzureContainer();
		azurePath = mConfig.getAzurePath();
	}

	public Handle<Void> upload(LogFilePath localPath) throws Exception {
		final String azureKey = localPath.withPrefix(azurePath).getLogFilePath();
		final File localFile = new File(localPath.getLogFilePath());

		final Future<Void> f = executor.submit(new Callable<Void>() {

			public Void call() throws Exception {
				LOG.info("uploading file {} ({} bytes) to azure://{}/{}",
						localFile, localFile.length(), azureContainer, azureKey);

				FileInputStream lf = null;
				try {
					// open the file for reading
					lf = new java.io.FileInputStream(localFile);

					// make sure the container exists
					CloudBlobContainer container = blobClient.getContainerReference(azureContainer);
					container.createIfNotExists();

					// set the timeout for the upload operation
					BlobRequestOptions options = new BlobRequestOptions();
					if (uploadTimeout > 0)
						options.setMaximumExecutionTimeInMs(uploadTimeout);

					// upload the blob
					CloudBlockBlob blob = container.getBlockBlobReference(azureKey);
					blob.upload(lf, localFile.length(), null, options, null);
				} finally {
					if (lf != null)
						lf.close();
				}
				return null;
			}

		});

		return new FutureHandle<Void>(f);
	}
}

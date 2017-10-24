package com.pinterest.secor.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

	/**
	 * Creates a cached threadpool executor with unbounded queue that will run at 
	 * most maxThreads threads. Threads are shutdown if idle for a while.
	 * 
	 * @param maxThreads maximum number of threads that will be used by the executor
	 * @return the created thread pool executor
	 */
	public static ThreadPoolExecutor createCachedThreadPool(int maxThreads) {
		ThreadPoolExecutor executor = new ThreadPoolExecutor(maxThreads, maxThreads, 
				60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		executor.allowCoreThreadTimeOut(true);
		return executor;
	}
	
	/**
	 * Creates a cached threadpool executor with unbounded queue that will run at 
	 * most maxThreads threads marked with the given name. Threads are shutdown if idle 
	 * for a while. 
	 * 
	 * @param maxThreads maximum number of threads that will be used by the executor
	 * @param name prefix assigned to the threads
	 * @return the created thread pool executor
	 */
	public static ThreadPoolExecutor createCachedThreadPool(int maxThreads, final String name) {
		ThreadPoolExecutor executor = createCachedThreadPool(maxThreads);
		
		final ThreadFactory tf = executor.getThreadFactory();
		executor.setThreadFactory(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = tf.newThread(r);
				t.setName(name + "-" + t.getId());
				return t;
			}
		});
		
		return executor;
	}
	
}

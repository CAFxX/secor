package com.pinterest.secor.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolUtil {
	
	private static int defaultIdleTimeout = 60;

	/**
	 * Creates a cached threadpool executor with unbounded queue that will run at 
	 * most maxThreads threads. Threads are shutdown if idle for a while.
	 * 
	 * @param maxThreads maximum number of threads that will be used by the executor
	 * @return the created thread pool executor
	 */
	public static ThreadPoolExecutor createCachedThreadPool(int maxThreads) {
		return createCachedThreadPool(maxThreads, defaultIdleTimeout);
	}

	/**
	 * Creates a cached threadpool executor with unbounded queue that will run at 
	 * most maxThreads threads. Threads are shutdown if idle for idleTimeout seconds.
	 * 
	 * @param maxThreads maximum number of threads that will be used by the executor
	 * @param idleTimeouts how many seconds threads should stay alive waiting for work
	 * @return the created thread pool executor
	 */
	public static ThreadPoolExecutor createCachedThreadPool(int maxThreads, int idleTimeout) {
		ThreadPoolExecutor executor = new ThreadPoolExecutor(maxThreads, maxThreads, 
				idleTimeout, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
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
		return createCachedThreadPool(maxThreads, name, defaultIdleTimeout );
	}

	/**
	 * Creates a cached threadpool executor with unbounded queue that will run at 
	 * most maxThreads threads marked with the given name. Threads are shutdown if idle 
	 * for idleTimeout seconds. 
	 * 
	 * @param maxThreads maximum number of threads that will be used by the executor
	 * @param name prefix assigned to the threads
	 * @param idleTimeouts how many seconds threads should stay alive waiting for work
	 * @return the created thread pool executor
	 */
	public static ThreadPoolExecutor createCachedThreadPool(int maxThreads, final String name, int idleTimeout) {
		ThreadPoolExecutor executor = createCachedThreadPool(maxThreads, idleTimeout);
		final ThreadFactory tf = executor.getThreadFactory();

		executor.setThreadFactory(new ThreadFactory() {
			final AtomicInteger cnt = new AtomicInteger();

			@Override
			public Thread newThread(Runnable r) {
				Thread t = tf.newThread(r);
				t.setName(name + "-" + cnt.incrementAndGet());
				return t;
			}
		});
		
		return executor;
	}
	
}

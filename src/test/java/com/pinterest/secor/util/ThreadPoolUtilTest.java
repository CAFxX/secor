package com.pinterest.secor.util;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class ThreadPoolUtilTest {
    @Test
    public void testThreadPoolSetup() {
    	ThreadPoolExecutor tp = ThreadPoolUtil.createCachedThreadPool(256);
    	Assert.assertEquals(256, tp.getMaximumPoolSize());
    	Assert.assertEquals(256, tp.getCorePoolSize());
    	Assert.assertEquals(60, tp.getKeepAliveTime(TimeUnit.SECONDS));
    	Assert.assertTrue(tp.allowsCoreThreadTimeOut());
    }
    
    @Test
    public void testThreadPoolSetupWithTimeout() {
    	ThreadPoolExecutor tp = ThreadPoolUtil.createCachedThreadPool(4, 1);
    	Assert.assertEquals(4, tp.getMaximumPoolSize());
    	Assert.assertEquals(4, tp.getCorePoolSize());
    	Assert.assertEquals(1, tp.getKeepAliveTime(TimeUnit.SECONDS));
    	Assert.assertTrue(tp.allowsCoreThreadTimeOut());
    }
    
    @Test
    public void testThreadPool() throws InterruptedException {
    	testThreadPool(ThreadPoolUtil.createCachedThreadPool(4, 1));
    }
    
    private void testThreadPool(ThreadPoolExecutor tp) throws InterruptedException {
    	Assert.assertEquals(1, tp.getKeepAliveTime(TimeUnit.SECONDS));
    	
    	// queue 4 tasks, all of which will immediately start on a thread
    	for (int i=0; i<4; i++) {
        	Assert.assertEquals(i, tp.getPoolSize());
        	Assert.assertEquals(0, tp.getQueue().size());
    		tp.submit(new Sleeper(200));
    	}
    	Assert.assertEquals(4, tp.getPoolSize());
    	Assert.assertEquals(0, tp.getQueue().size());

    	// queue 4 more tasks, that will have to wait because all threads are busy
    	for (int i=0; i<4; i++) {
        	Assert.assertEquals(4, tp.getPoolSize());
        	Assert.assertEquals(i, tp.getQueue().size());
    		tp.submit(new Sleeper(200));
    	}   	
    	Assert.assertEquals(4, tp.getPoolSize()); // 4 threads running
    	Assert.assertEquals(4, tp.getQueue().size()); // 4 tasks queued
    	Assert.assertEquals(0, tp.getCompletedTaskCount());
    	
    	Thread.sleep(300); 
    	Assert.assertEquals(4, tp.getPoolSize()); // 4 threads running
    	Assert.assertEquals(0, tp.getQueue().size()); // all 4 tasks were dequeued
    	Assert.assertEquals(4, tp.getCompletedTaskCount()); // 4 tasks completed
    	
    	Thread.sleep(2000); // worst case
    	Assert.assertEquals(0, tp.getPoolSize()); // no more threads are running
    	Assert.assertEquals(0, tp.getQueue().size());
    	Assert.assertEquals(8, tp.getCompletedTaskCount()); // all tasks completed
    }
    
    @Test
    public void testNamedThreadPool() throws InterruptedException {
    	ThreadPoolExecutor tp = ThreadPoolUtil.createCachedThreadPool(4, "yadda", 1);
    	
    	Thread t = tp.getThreadFactory().newThread(new Sleeper(0));
    	Assert.assertEquals("yadda-1", t.getName());
    	t = tp.getThreadFactory().newThread(new Sleeper(0));
    	Assert.assertEquals("yadda-2", t.getName());
    	
    	testThreadPool(tp); // for the rest, it's the same as the unnamed version
    }
    
    private class Sleeper implements Runnable {
    	private int millis;

		public Sleeper(int millis) {
    		this.millis = millis;
    	}
    	
		@Override
		public void run() {
			try {
				Thread.sleep(millis);
			} catch (InterruptedException e) {
				Assert.fail();
			}	
		}
    } 
}

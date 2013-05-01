package me.tatetian.hs.benchmark;

import me.tatetian.hs.benchmark.Task;

public class TestTask extends junit.framework.TestCase {
	public void testExtendingTask() {
		Task t = new Task("Task Just for Testing") {
			@Override
			protected void doRun() {
				try {
					Thread.sleep(1000);	// do nothing, just sleep 1 second
				} catch (InterruptedException e) {
					e.printStackTrace();
				}	
			}
		};
		t.run();
	}
}

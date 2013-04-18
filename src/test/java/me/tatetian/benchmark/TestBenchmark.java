package me.tatetian.benchmark;

public class TestBenchmark extends junit.framework.TestCase {
	public void testExtendingBenchmark() {
		class ExtendedBenchmark extends Benchmark {
			public ExtendedBenchmark(String name) {
				super(name);
				
				addTask(new Task("Task 1") {
					@Override
					protected void doRun() {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
				
				addTask(new Task("Task 2") {
					@Override
					protected void doRun() {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
			}
		}
		Benchmark bm = new ExtendedBenchmark("Extended Benchmark Just for Testing");
		bm.run();
	}
}

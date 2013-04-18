package me.tatetian.benchmark;

public abstract class Task {
	private long runningTime = -1;
	private String name = null;
	
	protected abstract void doRun();
	
	public Task(String name) {
		this.name = name;
	}
	
	public void run() {
		before();
		runningTime = System.currentTimeMillis();
		doRun();
		runningTime = System.currentTimeMillis() - runningTime;
		after();
		result();
	}
	
	public String getName() {
		return name;
	}
	
	public long getRunningTime() {
		return runningTime;
	}

	protected void result() {
		// nop
	}
	
	protected void before() {
		log("Running job `%s`...\n", name);
	}
	
	protected void after() {
		log("Finished job `%s` (in %f seconds)\n", name, runningTime / 1000.0f);
	}
	
	private void log(String format, Object ... args) {
		System.out.format("[Task] " + format, args);
	}
}

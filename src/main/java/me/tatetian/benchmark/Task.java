package me.tatetian.benchmark;

public abstract class Task {
  private static final int NUM_REPEATS = 5;
  private static final boolean SKIP_FIRST = true;

	private long runningTime = -1;
  private long totalTime = -1;
	private String name = null;
	
	protected abstract void doRun();
	
	public Task(String name) {
		this.name = name;
	}
	
	public void run() {
    if(SKIP_FIRST) doRun();

    totalTime = 0;
    for(int i = 0; i <= NUM_REPEATS; i++) { 
      before();
      runningTime = System.currentTimeMillis();
      doRun();
      runningTime = System.currentTimeMillis() - runningTime;
      totalTime += runningTime;
      after();
      result();
    }
    log("Average running time is %f seconds\n", totalTime / NUM_REPEATS / 1000.0f );
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

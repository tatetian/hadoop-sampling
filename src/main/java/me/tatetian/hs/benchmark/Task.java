package me.tatetian.hs.benchmark;

public abstract class Task {
	private long runningTime = -1;
	private String name = null;
	private boolean logging = true;
	
	protected String description = null;
	
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
	
	public void setDescription(String description) {
		this.description = description;
	} 
	
	public String getDescription() {
		return description;
	}
	
	public long getRunningTime() {
		return runningTime;
	}

	protected void result() {
		// nop
	}
	
	protected void before() {
		log("Running job `%s`...", name);
		if(description != null) log(description);
	}
	
	protected void after() {
		log("Finished job `%s` (in %f seconds)", name, runningTime / 1000.0f);
	}
	
	protected void log(String format, Object ... args) {
		if(logging) System.out.format("[Task] " + format + "\n", args);
	}
	
	protected void disalbeLog() {
		logging = false;
	}
	
	protected void enableLog() {
		logging = true;
	}
}

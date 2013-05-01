package me.tatetian.hs.benchmark;

import java.util.ArrayList;
import java.util.List;

public abstract class Benchmark extends Task {
	protected static final int DEFAULT_NUM_REPEATS = 1;
  private int numRepeats = 0;
  private static final boolean SKIP_FIRST = true;
	
	public Benchmark(String name) {
		this(name, DEFAULT_NUM_REPEATS);
	}
	
	public Benchmark(String name, int numRepeats) {
		super(name);
		this.numRepeats = numRepeats;
	}
	
	@Override
	protected void doRun() {
		for(Task t : tasks) {
			// To warn up cache and enable JVM's JIT
			t.disalbeLog();
			t.run();
			t.enableLog();
			
			long totalTime = 0;
			for(int i = 0; i < numRepeats; i++) {
				t.run();
				totalTime += t.getRunningTime();
				System.gc();
			}
			t.log("Average running time of `%s` is %f seconds", t.getName(), totalTime / numRepeats / 1000.0f );
		}
	}

	@Override
	protected void before() {
		log("Running benchmark `%s`...", getName());
		if(description != null) log(description);
	}
	
	@Override
	protected void after() {
		log("Finished benchmark `%s` (in total %f seconds)", getName(), getRunningTime() / 1000.0f);
	}
	
	protected void addTask(Task task) {
		tasks.add(task);
	} 
	
	protected void log(String format, Object ... args) {
		System.out.format("[Benchmark] " + format + "\n", args);
	}
	
	protected List<Task> tasks = new ArrayList<Task>();
}

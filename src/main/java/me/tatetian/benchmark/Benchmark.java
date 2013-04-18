package me.tatetian.benchmark;

import java.util.ArrayList;
import java.util.List;

public abstract class Benchmark extends Task {
	public Benchmark(String name) {
		super(name);
	}
	
	@Override
	protected void doRun() {
		for(Task t : tasks) {
			t.run();
			System.gc();
		}
	}

	@Override
	protected void before() {
		log("Running benchmark `%s`...\n", getName());
	}
	
	@Override
	protected void after() {
		log("Finished benchmark `%s` (in total %f seconds)\n", getName(), getRunningTime() / 1000.0f);
	}
	
	protected void addTask(Task task) {
		tasks.add(task);
	} 
	
	private void log(String format, Object ... args) {
		System.out.format("[Benchmark] " + format, args);
	}
	
	protected List<Task> tasks = new ArrayList<Task>();
}

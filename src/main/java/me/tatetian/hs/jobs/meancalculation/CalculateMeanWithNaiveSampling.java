package me.tatetian.hs.jobs.meancalculation;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

public class CalculateMeanWithNaiveSampling extends CalculateMean {
	@Override
	protected void customConfiguration(Job job) {	
		String msg = "!!!!!!with naive!!!!!!";
		System.out.println(msg);
		System.err.println(msg);
		job.setJarByClass(CalculateMeanWithNaiveSampling.class);
		job.setMapperClass(CalculateMeanMapperWithNaiveSampling.class);
	}
				
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CalculateMeanWithNaiveSampling(), args);
		System.exit(res);
	}
}

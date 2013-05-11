package me.tatetian.hs.jobs.meancalculation;

import org.apache.hadoop.mapreduce.Job;

public class CalculateMeanWithNaiveSampling extends CalculateMean {
	@Override
	protected void customConfiguration(Job job) {	
		String msg = "!!!!!!with naive!!!!!!";
		System.out.println(msg);
		System.err.println(msg);
		job.setJarByClass(CalculateMeanWithNaiveSampling.class);
		job.setMapperClass(CalculateMeanMapperWithNaiveSampling.class);
	}
}

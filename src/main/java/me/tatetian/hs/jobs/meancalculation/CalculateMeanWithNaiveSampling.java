package me.tatetian.hs.jobs.meancalculation;

import org.apache.hadoop.mapreduce.Job;

public class CalculateMeanWithNaiveSampling extends CalculateMean {
	@Override
	protected void customConfiguration(Job job) {	
		job.setJarByClass(CalculateMeanWithNaiveSampling.class);
		job.setMapperClass(CalculateMeanMapperWithNaiveSampling.class);
	}
}

package me.tatetian.hs.jobs.meancalculation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class CalculateMeanWithNaiveSampling extends CalculateMean {
	private float samplingRatio = 1.0f;
	
	public CalculateMeanWithNaiveSampling(float samplingRatio) {
		this.samplingRatio = samplingRatio;
	}
	
	@Override
	protected void customConfiguration(Job job) {	
		Configuration conf = job.getConfiguration();
		conf.setFloat("cps.sampling.ratio", samplingRatio);
		
		job.setMapperClass(CalculateMeanMapperWithNaiveSampling.class);
	}
}

package me.tatetian.hs.jobs.meancalculation;

import me.tatetian.hs.io.IndexedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class CalculateMeanWithIndexedSampling extends CalculateMean {
	private float samplingRatio = 1.0f;
	
	public CalculateMeanWithIndexedSampling(float samplingRatio) {
		if(samplingRatio < 0 || samplingRatio > 1) 
			throw new IllegalArgumentException("samplingRatio must be between 0 and 1");
		this.samplingRatio = samplingRatio;
	}
	
	@Override
	protected void customConfiguration(Job job) {
		Configuration conf = job.getConfiguration();
		conf.setFloat("cps.sampling.ratio", samplingRatio);
		job.setInputFormatClass(IndexedTextInputFormat.class);
	}
}
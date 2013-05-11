package me.tatetian.hs.jobs.meancalculation;

import me.tatetian.hs.io.IndexedTextInputFormat;

import org.apache.hadoop.mapreduce.Job;

public class CalculateMeanWithIndexedSampling extends CalculateMean {
	@Override
	protected void customConfiguration(Job job) {
		String msg = "!!!!!!with index!!!!!!";
		System.out.println(msg);
		System.err.println(msg);
		job.setInputFormatClass(IndexedTextInputFormat.class);
	}
}
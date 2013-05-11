package me.tatetian.hs.jobs.meancalculation;

import me.tatetian.hs.io.IndexedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class CalculateMeanWithIndexedSampling extends CalculateMean {
	@Override
	protected void customConfiguration(Job job) {
		job.setInputFormatClass(IndexedTextInputFormat.class);
	}
}
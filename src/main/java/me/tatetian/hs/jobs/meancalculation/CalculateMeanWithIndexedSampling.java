package me.tatetian.hs.jobs.meancalculation;

import me.tatetian.hs.io.IndexedTextInputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

public class CalculateMeanWithIndexedSampling extends CalculateMean {
	@Override
	protected void customConfiguration(Job job) {
		String msg = "!!!!!!with index!!!!!!";
		System.out.println(msg);
		System.err.println(msg);
		job.setInputFormatClass(IndexedTextInputFormat.class);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CalculateMeanWithIndexedSampling(), args);
		System.exit(res);
	}
}
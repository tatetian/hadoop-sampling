package me.tatetian.hs.jobs.meancalculation;

import me.tatetian.hs.io.IOConfigKeys;
import me.tatetian.hs.io.IndexedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

public class CalculateMeanWithIndexedSampling extends CalculateMean {
	@Override
	protected void customConfiguration(Job job) {
		String msg = "!!!!!!with index!!!!!!";
		System.out.println(msg);
		System.err.println(msg);
		job.setInputFormatClass(IndexedTextInputFormat.class);

		Configuration conf = job.getConfiguration();
		
		// Optimize DfsClient performance by enabling local reading shortcircuit
		conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
		conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, true);
		conf.setInt(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY, 4096);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CalculateMeanWithIndexedSampling(), args);
		System.exit(res);
	}
}
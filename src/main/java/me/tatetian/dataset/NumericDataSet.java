package me.tatetian.dataset;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;

import me.tatetian.cps.io.CascadingSampledDataOutputStream;

public class NumericDataSet extends DataSet {
	public NumericDataSet(int numRecords) {
		super(numRecords);
	}

	@Override
	public void dump(FSDataOutputStream out) throws IOException {
		long seed = 100L;
		Random random = new Random(seed);		
		for(int i = 0; i < numRecords; i++) {
			random.nextLong();
			out.writeBytes(Long.toString(random.nextLong()));
			out.writeByte('\n');
		}
		out.close();
	}

	@Override
	public void dump(CascadingSampledDataOutputStream out) throws IOException {
		long seed = 100L;
		Random random = new Random(seed);
		for(int i = 0; i < numRecords; i++) {
			random.nextLong();
			out.writeBytes(Long.toString(random.nextLong()));
			out.writeSeparator();	
		}
		out.close();	
	}
}

package me.tatetian.dataset;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import me.tatetian.cps.io.CascadingSampledDataOutputStream;

public class NumericDataSet extends DataSet {
	public NumericDataSet(int numRecords) {
		super(numRecords);
	}

	@Override
	public void dump(FSDataOutputStream out) throws IOException {
		for(int i = 0; i < numRecords; i++) {
			for(int j = 0; j < 10; j++) {
				out.writeBytes(Long.toString(i));
				out.write(' ');
			}
			out.writeByte('\n');
		}
		out.close();
	}

	@Override
	public void dump(CascadingSampledDataOutputStream out) throws IOException {
		for(int i = 0; i < numRecords; i++) {
			for(int j = 0; j < 10; j++) {
				out.writeBytes(Long.toString(i));
				out.write(' ');
			}
			out.writeSeparator();	
		}
		out.close();	
	}
}

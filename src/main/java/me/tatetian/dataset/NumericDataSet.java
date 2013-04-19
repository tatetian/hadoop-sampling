package me.tatetian.dataset;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import me.tatetian.cps.io.CascadingSampledDataOutputStream;

public class NumericDataSet extends DataSet {
	public NumericDataSet(int numRecords) {
		super(numRecords);
	}

	private final String record = "[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds";
	private final byte[] recordBytes = record.getBytes();
	
	@Override
	public void dump(FSDataOutputStream out) throws IOException {
		
		for(int i = 0; i < numRecords; i++) {
//			for(int j = 0; j < 10; j++) {
//				out.writeBytes(Long.toString(i));
//				out.write(' ');
//			}
			
			out.write(record.getBytes());
			out.writeByte('\n');
		}
		out.close();
	}

	@Override
	public void dump(CascadingSampledDataOutputStream out) throws IOException {
		for(int i = 0; i < numRecords; i++) {
//			for(int j = 0; j < 10; j++) {
//				out.writeBytes(Long.toString(i));
//				out.write(' ');
//			}
			//out.writeBytes("[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds");
			out.write(record.getBytes());
			out.writeSeparator();	
		}
		out.close();	
	}
}

package me.tatetian.hs.dataset;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import me.tatetian.hs.io.CascadingSampledDataOutputStream;

public class DataSet {
	protected int numRecords = 0;
	protected FieldGenerator[] fieldGenerators = null;
	
	public DataSet(FieldGenerator[] fieldGenerators ) {
		this(100, fieldGenerators);
	}
	
	public DataSet(int numRecords, FieldGenerator[] fieldGenerators ) {
		this.numRecords = numRecords;
		this.fieldGenerators = fieldGenerators;
	} 
	
	public int getNumRecords() {
		return numRecords;
	}
	
	public void setNumReords(int numRecords) {
		this.numRecords = numRecords;
	}
	
	public FieldGenerator[] getFieldGenerators() {
		return fieldGenerators;
	}
	
	public void dump(FSDataOutputStream out) throws IOException {
		try {
			for(int i = 0; i < numRecords; i++) {
				for(int j = 0; j < fieldGenerators.length; j++) {
					if(j != 0) out.write('\t');
					out.write(fieldGenerators[j].next());
				}
				out.writeByte('\n');
			}
		}
		finally {
			out.close();
		}
	}

	public void dump(CascadingSampledDataOutputStream out) throws IOException {
		for(int i = 0; i < numRecords; i++) {
			for(int j = 0; j < fieldGenerators.length; j++) {
				if(j != 0) out.write('\t');
				out.write(fieldGenerators[j].next());
			}
			out.writeSeparator();	
		}
		out.close();
	}
}

package me.tatetian.dataset;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import me.tatetian.cps.io.CascadingSampledDataOutputStream;

public abstract class DataSet {
	protected int numRecords = 0;
	
	public DataSet(int numRecords) {
		this.numRecords = numRecords;
	} 
	
	public int getNumRecords() {
		return numRecords;
	}
	
	public void setNumReords(int numRecords) {
		this.numRecords = numRecords;
	}
	
	public abstract void dump(FSDataOutputStream out) throws IOException;
	public abstract void dump(CascadingSampledDataOutputStream out) throws IOException;
}

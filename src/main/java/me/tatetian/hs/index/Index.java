package me.tatetian.hs.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class Index implements Writable {
	private short[] recordLens;
	private int numRecords = 0;
	
	private static final short[] EMPTY_SHORT_ARRAY = new short[0];
	
	public Index() {
		recordLens = EMPTY_SHORT_ARRAY;
	}

	public void clear() {
		numRecords = 0;
	}
	
	public int size() {
		return numRecords;
	}
	
	public int capacity() {
		return recordLens.length;
	}
	
	public void add(int recordLen) {
		int capacity = capacity();
		if(numRecords >= capacity) {
			capacity = capacity < 1024 ? 1024 : capacity * 2;
			recordLens = Arrays.copyOf(recordLens, capacity);
		}
		recordLens[numRecords++] = (short) recordLen;
	}
	
	public int get(int index) {
		return recordLens[index];
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(numRecords);
		for(int i = 0; i < numRecords; i++)
			out.writeShort(recordLens[i]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		numRecords = in.readInt();
		
		if(numRecords > recordLens.length)
			recordLens = new short[numRecords];
		for(int i = 0; i < numRecords; i++)
			recordLens[i] = in.readShort();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		int printLimit = Math.min(5, size());
		sb.append('{');
		for(int i = 0; i < printLimit; i++) {
			if(i != 0) sb.append(',');
			sb.append(get(i));
		}
		if(size() > printLimit) sb.append("...");
		sb.append('}');
		return sb.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof Index)) return false;
		
		Index another = (Index) o;
		
		// check size
		int size = size();
		if(size != another.size()) return false;
		// check content
		for(int i = 0; i < size; i++)
			if(recordLens[i] != another.recordLens[i]) return false;
		
		return true;
	}
}
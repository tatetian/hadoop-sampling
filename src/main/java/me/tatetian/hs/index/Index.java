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
	private ByteBuffer buff = null;
	
	public static Index createIndex(Configuration conf) {
		int maxSize = getMaxIndexSize(conf);
		return new Index(maxSize);
	} 
	
	private static int getMaxIndexSize(Configuration conf) {
		 int blockSize = conf.getInt("dfs.blocksize", 32 * 1024 * 1024); 
		 return conf.getInt("cps.indexing.size.max", blockSize) / 8;
	}
	
	public Index(int maxSize) {
		buff = ByteBuffer.allocate(maxSize);
		buff.order(ByteOrder.nativeOrder());	// always use native order
	}

	public void clear() {
		buff.clear();
	}
	
	public int size() {
		assert(buff.position() % 4 == 0);
		return buff.position() / 4;
	}
	
	public void add(int recordLen) {
		buff.putInt(recordLen);
	}
	
	public int get(int index) {
		return buff.getInt(index << 2);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		int len = buff.position();
		out.writeInt(len);
		out.write(buff.array(), 0, len);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		buff.clear();
		int len = in.readInt();		
		assert(len % 4 == 0);
		in.readFully(buff.array(), 0, len);
		buff.position(len);
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
		return Arrays.equals(buff.array(), another.buff.array());
	}
}
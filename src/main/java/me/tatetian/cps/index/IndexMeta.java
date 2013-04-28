package me.tatetian.cps.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IndexMeta implements Writable {
	public long dataBlockOffset = -1;
	public long dataBlockLen = -1;
	public int 	dataRecordNum = -1;
	public long indexBlockOffset = -1;
	public long indexBlockLen = -1;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(dataBlockOffset);
		out.writeLong(dataBlockLen);
		out.writeInt(dataRecordNum);
		out.writeLong(indexBlockOffset);
		out.writeLong(indexBlockLen);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		dataBlockOffset = in.readLong();
		dataBlockLen = in.readLong();
		dataRecordNum = in.readInt();
		indexBlockOffset = in.readLong();
		indexBlockLen = in.readLong();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("<data_block_offset=").append(dataBlockOffset)
			.append(",data_block_len=").append(dataBlockLen)
			.append(",data_record_num=").append(dataRecordNum)
			.append(",index_block_offset=").append(indexBlockOffset)
			.append(",index_block_len=").append(indexBlockLen)
			.append(">");
		return sb.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof IndexMeta)) return false;
		
		IndexMeta another = (IndexMeta) o;
		return dataBlockOffset == another.dataBlockOffset &&
					 dataBlockLen	   == another.dataBlockLen &&
					 dataRecordNum	 == another.dataRecordNum &&
					 indexBlockOffset == another.indexBlockOffset &&
					 indexBlockLen 	 == another.indexBlockLen;
	}
}

package me.tatetian.hs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import me.tatetian.hs.index.IndexMeta;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class IndexedFileSplit extends FileSplit {
	private IndexMeta[] metas = null;

	public IndexedFileSplit() {
		super();
		metas = null;
	}
	
	public IndexedFileSplit(Path file, long start, long length,
													String hosts[], IndexMeta[] metas) {
		super(file, start, length, hosts);
		this.metas = metas;
		validate();
	}
	
	private void validate() {
		Path file 	= getPath();
		long start	= getStart();
		long length = getLength();
		
		if(file != null && length > 0) {
			// Check meta
			if(metas == null || metas.length == 0) 
				throw new IllegalArgumentException("No information about index meta is given");
			// Check start
			if(start != metas[0].dataBlockOffset)
				throw new IllegalArgumentException("Start of file split is inconsistent with index meta");
			// Check length
			long realLen = 0;
			for(int i = 0; i < metas.length; i++)
				realLen += metas[i].dataBlockLen;
			if(realLen < length)
				throw new IllegalArgumentException("Length of file split is inconsistent with index meta");
		}
	}
	
	public IndexMeta[] getIndexMeta() {
		return metas;
	}
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof IndexedFileSplit))
			return false;
		
		IndexedFileSplit another = (IndexedFileSplit) o;
		Path file0 			= getPath(), 			file1 	= another.getPath();
		long start0 		= getStart(), 		start1 	= another.getStart();
		long len0				= getLength(), 		len1		= another.getLength();
		String[] hosts0, hosts1;
		try {
			hosts0 = getLocations();
			hosts1 = another.getLocations();
		} catch (IOException e) {
			return false;
		}
		
		return len0 == len1 && start0 == start1 && 
					 (file0 	== null ? file1 == null : file0.equals(file1) ) &&
					 (hosts0 	== null ? hosts1 == null : Arrays.equals(hosts0, hosts1) ) &&
					 (metas		== null ? another.metas == null : Arrays.equals(metas, another.metas) );
	}
	
	@Override
	public String toString() {
		return super.toString() + "(" + metas.length + " blocks)"; 
	}
	
	@Override
  public void write(DataOutput out) throws IOException {
		super.write(out);
		
		if(metas != null) {
			out.writeInt(metas.length);
			for(int i = 0; i < metas.length; i++) {
				metas[i].write(out);
			}
		}
		else 
			out.writeInt(0);
	}

  @Override
  public void readFields(DataInput in) throws IOException {
  	super.readFields(in);
  	
  	int metaCount = in.readInt();
  	if(metaCount > 0) {
	  	metas = new IndexMeta[metaCount];
		  	for(int i = 0; i < metaCount; i++) {
	  		metas[i] = new IndexMeta();
	  		metas[i].readFields(in);
	  	}
  	}
  	else
  		metas = null;
  }
}

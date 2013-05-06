package me.tatetian.hs.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public abstract class AbstractIndexReader<V> implements java.io.Closeable  {
	protected SequenceFile.Reader reader = null;
	protected IntWritable blockId = new IntWritable(0);

	public AbstractIndexReader(Configuration conf, Path path) throws IOException {
		this(conf, path, 0);
	}
	
	public AbstractIndexReader(Configuration conf, Path path, long start) throws IOException {
		int buffSize = getBuffSize(conf);
		this.reader = new SequenceFile.Reader(conf, 
																					SequenceFile.Reader.file(path), 
																					SequenceFile.Reader.start(start),
																					SequenceFile.Reader.bufferSize(buffSize));
	}
	
	public boolean next(V val) throws IOException {
		return reader.next(blockId, (Writable)val);
	}

	public void seek(long pos) throws IOException {
		reader.seek(pos);
	}
	
	public long getPosition() throws IOException {
		return reader.getPosition();
	}
	
	@Override
	public void close() throws IOException {
		reader.close();
	}
	
	protected static int getBuffSize(Configuration conf) {
		return conf.getInt("io.file.buffer.size", 4096);
	}
}


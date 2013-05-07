package me.tatetian.hs.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Progressable;

public class AbstractIndexWriter<V>  implements java.io.Closeable, Syncable {
	private SequenceFile.Writer writer = null;
	private IntWritable blockId = new IntWritable(0);
	
	public AbstractIndexWriter(Configuration conf, Path path, Class valueClass, Progressable progress) throws IOException {
		int buffSize = getBuffSize(conf);
		this.writer = SequenceFile.createWriter(conf, 
																						SequenceFile.Writer.file(path), 
																						SequenceFile.Writer.keyClass(IntWritable.class),
																						SequenceFile.Writer.valueClass(valueClass), 
																						SequenceFile.Writer.bufferSize(buffSize), 
																						SequenceFile.Writer.progressable(progress));
	}
	
	public void append(V val) throws IOException {
		writer.append(blockId, val);
		blockId.set(blockId.get() + 1);
	}
	
	public long getPosition() throws IOException {
		return writer.getLength();
	}
	
	@Override
	@Deprecated
	public void sync() throws IOException {
		writer.sync();
	}

	@Override
	public void hflush() throws IOException {
		writer.hflush();
	}

	@Override
	public void hsync() throws IOException {
		writer.hsync();
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}
	
	protected static int getBuffSize(Configuration conf) {
		return conf.getInt("io.file.buffer.size", 4096);
	}
}
	
	


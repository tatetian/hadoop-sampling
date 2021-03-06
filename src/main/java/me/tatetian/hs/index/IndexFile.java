package me.tatetian.hs.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class IndexFile {
	public static class Reader extends AbstractIndexReader<Index> {
		public Reader(Configuration conf, Path path) throws IOException {
			this(conf, path, 0);
		}
		
		public Reader(Configuration conf, Path path, long start) throws IOException {
			super(conf, path, start);
		}
	}
	
	public static class Writer extends AbstractIndexWriter<Index> {
		public Writer(Configuration conf, Path path) throws IOException {
			this(conf, path, null);
		}
		public Writer(Configuration conf, Path path, Progressable progress) throws IOException {
			super(conf, path, Index.class, progress);
		}
	}
}

package me.tatetian.hs.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class IndexMetaFile {
	public static class Reader extends AbstractIndexReader<IndexMeta> {
		public Reader(Configuration conf, Path path) throws IOException {
			this(conf, path, 0);
		}
		
		public Reader(Configuration conf, Path path, long start) throws IOException {
			super(conf, path, start);
		}
	}
	
	public static class Writer extends AbstractIndexWriter<IndexMeta> {
		public Writer(Configuration conf, Path path) throws IOException {
			super(conf, path, IndexMeta.class);
		}
	}
}

package me.tatetian.cps.index;

import java.io.IOException;

import org.apache.hadoop.fs.Syncable;

public class IndexMetaFile {
	public static class Reader implements java.io.Closeable  {
		public boolean next(IndexMeta meta) {
			return false;
		}
		
		
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public static class Writer  implements java.io.Closeable, Syncable {

		@Override
		@Deprecated
		public void sync() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void hflush() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void hsync() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}
}

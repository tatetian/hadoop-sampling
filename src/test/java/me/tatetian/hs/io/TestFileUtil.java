package me.tatetian.hs.io;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFileUtil {
	@Test
	public void testIsContentIdentical() throws IOException {
		Configuration conf = new Configuration();
		Path path1 = new Path("tmp/p1.dat"),
				 path2 = new Path("tmp/p2.dat");
		FSDataOutputStream out1 = FileUtil.createFile(path1, conf),	
											 out2 = FileUtil.createFile(path2, conf);
		// Write same data to both files
		for(int i = 0; i < 1024; i++) {
			out1.write('*');
			out2.write('*');
		}
		// Close
		out1.close(); out2.close();
		// Should be same
		Assert.assertTrue(FileUtil.isContentIdentical(path1, path2, conf));
	}
	
	@Test
	public void testIsContentIdentical2() throws IOException {
		Configuration conf = new Configuration();
		Path path1 = new Path("tmp/p3.dat"),
				 path2 = new Path("tmp/p4.dat");
		FSDataOutputStream out1 = FileUtil.createFile(path1, conf),	
											 out2 = FileUtil.createFile(path2, conf);
		// Write same data to both files
		for(int i = 0; i < 1024; i++) {
			out1.write('*');
			out2.write('*');
		}
		out1.write('-');
		out2.write('?');
		// Close
		out1.close(); out2.close();
		// Should be same
		Assert.assertTrue(!FileUtil.isContentIdentical(path1, path2, conf));
	}
}

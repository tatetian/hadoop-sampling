package me.tatetian.hs.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import me.tatetian.hs.index.IndexMeta;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexedFileSplit {
	@Test
	public void testWriteRead() throws IOException {
		// Write content
		ByteArrayOutputStream out0 = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(out0);
		IndexedFileSplit splitWritten = new IndexedFileSplit(
																			new Path("sth"), 0L, 2048L, null, 
																			new IndexMeta[] {
																				new IndexMeta(0L, 1024L, 128, 0L, 512L),
																				new IndexMeta(1024L, 1024L, 128, 512L, 1024L)
																			});
		splitWritten.write(out);
		// Read content
		ByteArrayInputStream in0 = new ByteArrayInputStream(out0.toByteArray());
		DataInputStream in = new DataInputStream(in0);		
		IndexedFileSplit splitRead = new IndexedFileSplit();
		splitRead.readFields(in);
		// Check equality
		Assert.assertEquals(splitWritten, splitRead);
	}
}

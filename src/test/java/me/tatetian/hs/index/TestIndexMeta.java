package me.tatetian.hs.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import me.tatetian.hs.index.IndexMeta;

import org.junit.Assert;
import org.junit.Test;

public class TestIndexMeta {
	@Test
	public void testWritable() throws IOException {
		// Write content
		ByteArrayOutputStream out0 = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(out0);
		IndexMeta metaWritten = new IndexMeta();
		metaWritten.write(out);
		// Read content
		ByteArrayInputStream in0 = new ByteArrayInputStream(out0.toByteArray());
		DataInputStream in = new DataInputStream(in0);		
		IndexMeta metaRead = new IndexMeta();
		metaRead.readFields(in);
		// Check equality
		Assert.assertEquals(metaWritten, metaRead);
	}
	
	@Test
	public void testToString() {
		IndexMeta meta = new IndexMeta();
		System.out.println(meta);
	}
	
	@Test
	public void testEquals() {
		IndexMeta meta1 = new IndexMeta();
		IndexMeta meta2 = new IndexMeta();
		Assert.assertEquals(meta1, meta2);
		meta2.dataBlockLen = 2;
		Assert.assertTrue("Not the same", !meta1.equals(meta2));
	}
}

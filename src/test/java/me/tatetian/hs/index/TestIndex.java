package me.tatetian.hs.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import me.tatetian.hs.index.Index;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestIndex {
	@Test
	public void testWritable() throws IOException {
		// Write content
		ByteArrayOutputStream out0 = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(out0);
		Index indexWritten = new Index();
		indexWritten.add(1);
		indexWritten.add(2);
		indexWritten.write(out);
		// Read content
		ByteArrayInputStream in0 = new ByteArrayInputStream(out0.toByteArray());
		DataInputStream in = new DataInputStream(in0);		
		Index indexRead = new Index();
		indexRead.readFields(in);
		// Check equality
		Assert.assertEquals(indexWritten, indexRead);
	}

	@Test
	public void testEquality() {
		Index index1 = new Index();
		Index index2 = new Index();
		Assert.assertEquals(index1, index2);
		index1.add(1); index2.add(2);
		Assert.assertFalse(index1.equals(index2));
		index1.clear(); index2.clear();
		index1.add(3); index2.add(3);
		Assert.assertTrue(index1.equals(index2));
		index1.add(4);
		Assert.assertFalse(index1.equals(index2));
	}
	
	@Test
	public void testToString() {
		Configuration conf = new Configuration();
		Index index = new Index();
		index.add(1);
		index.add(2);
		index.add(3);
		Assert.assertEquals("{1,2,3}", index.toString());
		index.add(4);
		index.add(5);
		index.add(6);
		Assert.assertEquals("{1,2,3,4,5...}", index.toString());
	}
}

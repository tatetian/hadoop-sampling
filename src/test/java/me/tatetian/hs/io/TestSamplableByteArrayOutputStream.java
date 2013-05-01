package me.tatetian.hs.io;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import me.tatetian.hs.io.SamplableByteArrayOutputStream;
import me.tatetian.hs.sampler.BernoulliSampler;
import me.tatetian.hs.sampler.Sampler;
import me.tatetian.hs.sampler.SystematicSampler;

import junit.framework.Assert;

public class TestSamplableByteArrayOutputStream extends junit.framework.TestCase {
	//===========================================================================
	// Test Cases
	//===========================================================================
	
	/**
	 * Test the integrity of write operation
	 * */
	public void testWrite() throws IOException {
		// Init
		int N1 = 128, N2 = 256;
		// Do without sampling
		doTest(N1, N2, null);
	}
	
	/**
	 * Test the update operation by <code>BernoulliSampler</code>
	 * */
	public void testUpdateByBernoulliSampler() throws IOException {
		// Init
		int N1 = 4096, N2 = 2048, ratio = 16;
		BernoulliSampler sampler = new BernoulliSampler(ratio);
		// Do
		doTest(N1, N2, sampler);
	}
	
	/**
	 * Test the update operation by <code>SystematicSampler</code>
	 * */
	public void testUpdateBySystematicSampler() throws IOException {
		// Init
		int N1 = 10240, N2 = 777, period = 7;
		Sampler sampler = new SystematicSampler(period);
		// Do
		doTest(N1, N2, sampler);
	}
	
	//===========================================================================
	// Helper functions
	//===========================================================================
	
	/**
	 * General framework for test cases
	 * 
	 * Procedure: write => read & check => sample => read & check 
	 * 				 => write => read & check => sample => read & check
	 * The sample step can be skipped if sampler is null.
	 * */
	private void doTest(int N1, int N2, Sampler sampler) throws IOException {
		List<Integer> expected = new ArrayList<Integer>();
		// Open
		SamplableByteArrayOutputStream out = new SamplableByteArrayOutputStream();
		// Write and check
		write(out, N1, expected);
		readAndCheck(out, expected);
		// Sample & check
		if(sampler != null) {
			sample(out, sampler, expected);
			readAndCheck(out, expected);
		}
		// Write and check again!
		write(out, N2, expected);
		readAndCheck(out, expected);
		// Sample & check again!
		if(sampler != null) {
			sample(out, sampler, expected);
			readAndCheck(out, expected);
		}
	}
	
	/**
	 * Write a sequence number to stream
	 * */
	private void write(SamplableByteArrayOutputStream out, int N, List<Integer> expected) throws IOException {
		DataOutputStream out2 = new DataOutputStream(out);
		for(int i = 0; i < N; i++) {
			out.newRecord();
			out2.writeBytes(Integer.toString(i));
			out2.write("\n".getBytes("UTF-8"));
			expected.add(i);
		}
		out2.close();
	}
	
	/**
	 * Sample the records of an instance of <code>SamplableByteArrayOutputStream</code>
	 * */
	private void sample(SamplableByteArrayOutputStream out, Sampler sampler, List<Integer> expected) {
		// Generate an random seed for sampler
		Random random = new Random();
		long seed = random.nextLong();
		// Update out
		sampler.setSeed(seed);
		out.updateBy(sampler);
		// Get expected result
		sampler.setSeed(seed);
		int sampleSize = 0, total = expected.size();
		for(int i = 0; i < total; i++) {
			if(sampler.next()) {
				expected.set(sampleSize, expected.get(i));
				sampleSize ++;
			}
		}
		// Trim the list
		for(int i = total - 1; i >= sampleSize; i--) expected.remove(i);
	}
	
	/**
	 * Read an instance of <code>SamplableByteArrayOutputStream</code> and check
	 * the correctness of its records
	 * */
	private void readAndCheck(SamplableByteArrayOutputStream out, List<Integer> expected) throws IOException {
		// Open reader
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		// Read lines
		int lineCount = 0, num, expectedNum;
		String line = reader.readLine();
		
		while(line != null) {
			// Real
			num = Integer.parseInt(line);
			// Expected
			expectedNum = expected.get(lineCount).intValue();
			// Check equality
			Assert.assertEquals(expectedNum, num);
			
			lineCount ++;
			line = reader.readLine();
		}
		// Check size
		Assert.assertEquals(expected.size(), lineCount);
	}
}

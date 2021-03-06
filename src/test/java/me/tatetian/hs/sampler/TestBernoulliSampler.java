package me.tatetian.hs.sampler;

import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

import me.tatetian.hs.sampler.BernoulliSampler;
import junit.framework.Assert;

public class TestBernoulliSampler{
	//===========================================================================
	// Test Cases
	//===========================================================================
	
	/**
	 * Test the performance of sampler
	 * 
	 * For the naive implementation, it takes 14.624000 seconds to do 1 million 
	 * times of sampling on my machine; For amortized implementation, it takes only
	 * 4.891000 seconds, a speed-up of over 3X. 
	 * */
	@Test
	public void testPerformance() {
		int sampleRatio = 16;
		BernoulliSampler bs = new BernoulliSampler(sampleRatio);
		int N = 1000 * 1000 * 1000;
		long startTime = System.currentTimeMillis();
		for(int i = 0; i < N; i++) bs.next();
		long endTime = System.currentTimeMillis();
		float elapsedSeconds = (endTime - startTime) / 1000.0f;
		System.out.format("Takes %f seconds to do %d times of sampling\n", elapsedSeconds, N);
	}
	
	/**
	 * Test the sampler gives equal chance for each possibility of choice
	 **/
	@Test
	public void testUniform() {
		int sampleRatio = 16;
		BernoulliSampler bs = new BernoulliSampler(sampleRatio);
		int repeats = 10;
		int N = 1000000;
		// Do simulation repeats times
		for(int j = 0; j < repeats; j++) {
			// For a population unit, count # times of being sampled out of N trials
			int count = 0;
			for(int i = 0; i < N; i++) {
				if(bs.next()) count++;
			}
			assertApproxEquals(N/sampleRatio, count);
		}
	}
	
	/**
	 *  Test the sampler follows Bernoulli distribution
	 **/
	@Test
	public void testDistribution() {
		// Prepare sampler
		int sampleRatio = 16;													// proportion of sample
		BernoulliSampler bs = new BernoulliSampler(sampleRatio);
		// Take many Bernoulli samples
		int 	popuSize = 1024;												// population
		int 	sampleNum = 1024;												// # of samples
		int[] sampleSizeDist = new int[popuSize+1];		// distribution of sample sizes
		for(int i = 0; i < sampleNum; i++) {
			int sampleSize = 0;
			for(int j = 0; j < popuSize; j++) {
				if(bs.next()) sampleSize++;
			}
			sampleSizeDist[sampleSize] += 1;
		}
		// Calculate mean
		double total = 0;
		for(int i = 0; i < popuSize + 1; i++) {
			total += i * sampleSizeDist[i];
		}
		double realMean = total / sampleNum;
		double expectedMean = bs.getExpectedSampleSize(popuSize);
		System.out.println("Check mean:");
		assertApproxEquals(expectedMean, realMean);
		// Calculate variance
		double sqrSum = 0;
		for(int i = 0; i < popuSize + 1; i++) {
			sqrSum += sampleSizeDist[i] * (i - realMean) * (i - realMean);
		}
		double realStdVar = Math.sqrt((double)sqrSum / sampleNum);
		double expectedStdVar = bs.getStdVar(popuSize);
		System.out.println("Check std var:");
		assertApproxEquals(expectedStdVar, realStdVar);
		// Print distribution in ASCII
		System.out.println("Print distribution:");
		for(int i = 0; i < popuSize + 1; i++) {
			System.out.format("[%04d]", i);
			for(int j = 0; j < sampleSizeDist[i]; j++) System.out.print("=");
			System.out.print("\n");
		}
	}
	
	//===========================================================================
	// Helper functions
	//===========================================================================
	
	/**
	 *  Assert two numbers are approximately equal(within 10% by default)	 
	 **/
	private void assertApproxEquals(double expected, double real) {
		double error = 0.10;	// 10% by default
		assertApproxEquals(expected, real, error);
	}
	private void assertApproxEquals(double expected, double real, double error) {
		Assert.assertTrue(real + " ~= " + expected, 
											(1-error)*expected < real && real < (1+error)*expected);
	}
}

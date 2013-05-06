package me.tatetian.hs.sampler;

import org.junit.Test;

public class TestSkipSampler {
	/**
	 * Test the sampler gives equal chance for each possibility of choice
	 **/
	@Test
	public void testUniform() {
		float sampleRatio = 1.0f/16;
		SkipSampler bs = new SkipSampler(sampleRatio);
		int repeats = 10;
		int N = 10000;
		// Do simulation repeats times
		for(int j = 0; j < repeats; j++) {
			// For a population unit, count # times of being sampled out of N trials
			int count = 0;
			for(int i = bs.next(); i < N; i += bs.next()) {
				i ++;
				count++;
			}
			TestSamplerUtil.assertApproxEquals(N * sampleRatio, count);
		}
	}
	
	/**
	 *  Test the sampler follows Bernoulli distribution
	 **/
	@Test
	public void testDistribution() {
		// Prepare sampler
		float sampleRatio = 1.0f / 16;													// proportion of sample
		SkipSampler bs = new SkipSampler(sampleRatio);
		// Take many samples
		int 	popuSize = 1024;												// population
		int 	sampleNum = 1024;												// # of samples
		int[] sampleSizeDist = new int[popuSize+1];		// distribution of sample sizes
		for(int i = 0; i < sampleNum; i++) {
			int sampleSize = 0;
			for(int j = bs.next(); j < popuSize; j += bs.next() ) {
				sampleSize++;
				j ++;
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
		TestSamplerUtil.assertApproxEquals(expectedMean, realMean);
		// Calculate variance
		double sqrSum = 0;
		for(int i = 0; i < popuSize + 1; i++) {
			sqrSum += sampleSizeDist[i] * (i - realMean) * (i - realMean);
		}
		double realStdVar = Math.sqrt((double)sqrSum / sampleNum);
		double expectedStdVar = bs.getStdVar(popuSize);
		System.out.println("Check std var:");
		TestSamplerUtil.assertApproxEquals(expectedStdVar, realStdVar);
		// Print distribution in ASCII
		System.out.println("Print distribution:");
		for(int i = 0; i < popuSize + 1; i++) {
			System.out.format("[%04d]", i);
			for(int j = 0; j < sampleSizeDist[i]; j++) System.out.print("=");
			System.out.print("\n");
		}
	}
}

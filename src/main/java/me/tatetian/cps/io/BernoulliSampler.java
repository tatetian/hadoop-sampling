package me.tatetian.cps.io;

import java.util.Random;

/**
 *	This sampler implements Bernoulli sampling method, which ensures 
 *	equal probability for each population unit to be sampled.
 */
public class BernoulliSampler implements Sampler {
	/**
	 * Constructor
	 * @param samplingRatio each population unit has 1/samplingRatio chance of being sampled
	 **/
	public BernoulliSampler(int samplingRatio) {
		if(samplingRatio <= 0) throw new IllegalArgumentException();
		
		this.samplingRatio = samplingRatio;
		this.random = new Random();
	}
	
	/**
	 * Sample the next population unit? 	
	 **/
	@Override
	public boolean next() {
		return random.nextInt(samplingRatio) == 0; 
	}
	
	/**
	 * Set seed for random generator
	 * */
	@Override
	public void setSeed(long seed) {
		random.setSeed(seed);
	}
	
	/**
	 * Get expected sample size, which follows Binomial distribution
	 **/
	public double getExpectedSampleSize(int popuSize) {
		return popuSize / samplingRatio;
	}
	
	/**
	 * Get standard variance of sample size, which follows Binomial distribution
	 **/
	public double getStdVar(int popuSize) {
		return Math.sqrt((double)popuSize * samplingRatio / samplingRatio / samplingRatio);
	}
	
	/**
	 * Check whether the sample size likely by binomial distribution
	 * */
	public boolean isSizeLikely(int sampleSize, int popuSize) {
		double e = getExpectedSampleSize(popuSize),
					 v = getStdVar(popuSize);
		return e - 4 * v <= sampleSize && sampleSize <= e + 4 * v;
	}
	
	private int samplingRatio = 0;
	private Random random = null;
}

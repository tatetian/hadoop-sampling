package me.tatetian.cps.sampler;

import java.util.Random;

/**
 *	This sampler implements Bernoulli sampling method, which ensures 
 *	equal probability for each population unit to be sampled.
 */
public class BernoulliSampler implements Sampler {
	private double[] F;
	
	/**
	 * Constructor
	 * @param samplingRatio each population unit has 1/samplingRatio chance of being sampled
	 **/
	public BernoulliSampler(int samplingRatio) {
		if(samplingRatio <= 0) throw new IllegalArgumentException();
		
		this.samplingRatio = samplingRatio;
		this.random = new Random();
		
		// init F
		F = new double[2 * samplingRatio + 1];
		double p0 = 1.0 / samplingRatio, p = p0;
		F[0] = p;
		for(int i = 1; i < F.length; i++) {
			p = p * (1 - p0);
			F[i] = p + F[i-1];
		}
		F[F.length-1] = 1.0;
	}
	
	private int skip = -1;
	private boolean notSkipAll = false;
	
	/**
	 * Sample the next population unit? 	
	 **/
	@Override
	public boolean next() {
		/*if(skip < 0) {
			double f = random.nextDouble();
			skip = 0;
			while(f > F[skip]) skip++;
			notSkipAll = skip < F.length - 1;
		}
		return skip-- == 0 && notSkipAll;*/
		return random.nextInt() % samplingRatio == 0;
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

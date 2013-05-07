package me.tatetian.hs.sampler;

import java.util.Random;

public class SkipSampler {
	private double p;
	private Random random = new Random();
	
  private int skip;
	
	public SkipSampler(double p) {
		if(p < 0 || p > 1) throw new IllegalArgumentException("Probablity p must be within [0, 1]");
		
		this.p = p;
    skip = (int) Math.ceil(1 / p);
	}
	
	public int next() {
	/*	int skip = 0;
		while(random.nextDouble() > p) {
			skip ++;
		}
		return skip;*/
    return skip;
	}
	
	public double getExpectedSampleSize(int popSize) {
		return popSize * p;
	}
	
	public double getStdVar(int popSize) {
		return Math.sqrt( popSize * p * (1-p) );
	}
}

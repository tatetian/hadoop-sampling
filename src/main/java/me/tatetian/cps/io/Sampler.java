package me.tatetian.cps.io;

public interface Sampler {
	/**
	 * Decide whether to sample the next item by sampler's algorithm 
	 * */
	public boolean next();
	/**
	 * Set seed 
	 * 
	 * Usage: Fix seed so that sampling result can be repeated.
	 * */
	public void setSeed(long seed) ;
}

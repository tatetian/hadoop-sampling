package me.tatetian.hs.sampler;

import org.junit.Assert;

public class TestSamplerUtil {
	/**
	 *  Assert two numbers are approximately equal(within 10% by default)	 
	 **/
	public static void assertApproxEquals(double expected, double real) {
		double error = 0.10;	// 10% by default
		assertApproxEquals(expected, real, error);
	}
	public static void assertApproxEquals(double expected, double real, double error) {
		Assert.assertTrue(real + " ~= " + expected, 
											(1-error)*expected < real && real < (1+error)*expected);
	}
}

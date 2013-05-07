package me.tatetian.hs.jobs.meancalculation;

import org.apache.hadoop.conf.Configuration;

public class TestCalculateMeanWithNaiveSampling extends TestCalculateMean {
	@Override
	protected CalculateMean getCalculateMeanInstance() {
		float samplingRatio = 0.01f;
		return new CalculateMeanWithNaiveSampling(samplingRatio);
	}
}

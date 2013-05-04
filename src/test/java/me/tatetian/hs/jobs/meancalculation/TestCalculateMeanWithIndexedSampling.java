package me.tatetian.hs.jobs.meancalculation;

public class TestCalculateMeanWithIndexedSampling extends TestCalculateMean {
	@Override
	protected CalculateMean getCalculateMeanInstance() {
		float samplingRatio = 0.05f;
		return new CalculateMeanWithIndexedSampling(samplingRatio);
	}
}

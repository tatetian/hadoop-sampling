package me.tatetian.hs.dataset;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;

public class DataSetFactory {
	public static DataSet makeRepeatedString() {
		String record = "[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds[Task] Average running time of `Loading File with Sampling` is 27.969999 seconds";
		FieldGenerator[] fieldGenerators = new FieldGenerator[]{ 
				new RepeatingGenerator(record) 
		};
		return new DataSet(fieldGenerators);
	}
	
	public static DataSet makeNormalDist(double mean, double sd) {
		FieldGenerator[] fieldGenerators = new FieldGenerator[]{ 
				new RealGenerator(new NormalDistribution(mean, sd))
		};
		return new DataSet(fieldGenerators);
	}
	
	private static class RealGenerator implements FieldGenerator {
		private byte[] buff = new byte[100];
		private RealDistribution dist = null;
		
		public RealGenerator(RealDistribution dist) {
			this.dist = dist;
		}
		
		public RealDistribution getDistribution() {
			return dist;
		}
		
		@Override
		public byte[] next() {
//			double value = dist.sample();
//			long v = Double.doubleToLongBits(value);
//      buff[0] = (byte)(v >>> 56);
//      buff[1] = (byte)(v >>> 48);
//      buff[2] = (byte)(v >>> 40);
//      buff[3] = (byte)(v >>> 32);
//      buff[4] = (byte)(v >>> 24);
//      buff[5] = (byte)(v >>> 16);
//      buff[6] = (byte)(v >>>  8);
//      buff[7] = (byte)(v >>>  0);
//			return buff;
			byte[] res = null;
			try {
				res = nextString().getBytes();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return res;
		}

		@Override
		public String nextString() {
			double value = dist.sample();
			return Double.toString(value);
		}
	}
	
	private static class RepeatingGenerator implements FieldGenerator {
		private byte[] content;
		private String contentAsString;
		
		public RepeatingGenerator(String fieldContent) {
			content = fieldContent.getBytes();
			contentAsString = fieldContent;
		}
		
		@Override
		public byte[] next() {
			return content;
		}

		@Override
		public String nextString() {
			return contentAsString;
		}
	}
}

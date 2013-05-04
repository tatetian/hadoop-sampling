package me.tatetian.hs.jobs.meancalculation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class Pair implements Writable{
		private int count = 0;
		private double sum = 0.0;
		private int hashValue = 0;	// 0 indicates the hash value is not calculated yet
		
		public Pair() {
		}
		
		public Pair(int count, double sum) {
			this.count = count;
			this.sum = sum;
		}
		
		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Pair))
	      return false;
	    Pair p = (Pair)o;
	    return this.count == p.count && this.sum == p.sum;
		}
		
		@Override
	  public int hashCode() {
			if(hashValue == 0) {
				long[] vals = new long[]{count, Double.doubleToLongBits(sum)}; 
				hashValue = Arrays.hashCode(vals);
			}
			return hashValue;
	  }
		
		@Override
		public String toString() {
			return "<" + count + "," + sum + ">";
		}
		
		public int getCount() {
			return count;
		}
		
		public double getSum() {
			return sum;
		}
		
		public double getAverage() {
			return sum / count;
		}
		
		public void addCount(int count) {
			this.count += count;
		}
		
		public void addSum(double sum) {
			this.sum += sum;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(count);
			out.writeDouble(sum);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readInt();
			sum = in.readDouble();
		}
	}
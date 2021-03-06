package me.tatetian.hs.jobs.meancalculation;

import java.io.IOException;

import me.tatetian.hs.io.Text;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateMeanReducer extends Reducer<NullWritable, Pair, Text, DoubleWritable> {
	@Override
	protected void reduce(NullWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		double sum = 0.0;
		for(Pair v : values) {
			count += v.getCount();
			sum += v.getSum();
		}
		context.write(new Text("count"), new DoubleWritable(count));
		context.write(new Text("sum"), new DoubleWritable(sum));
		context.write(new Text("average"), new DoubleWritable(sum / count));
	}
}
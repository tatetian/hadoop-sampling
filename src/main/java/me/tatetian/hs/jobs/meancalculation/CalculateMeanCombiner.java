package me.tatetian.hs.jobs.meancalculation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateMeanCombiner extends Reducer<NullWritable, Pair, NullWritable, Pair> {
	@Override
	protected void reduce(NullWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
		Pair res = new Pair();
		Iterator<Pair> pairs = values.iterator();
		while(pairs.hasNext()) {
			Pair p = pairs.next();
			res.addCount(p.getCount());
			res.addSum(p.getSum());
		}
		context.write(NullWritable.get(), res);
	}
}

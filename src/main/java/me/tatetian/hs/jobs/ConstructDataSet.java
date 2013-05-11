package me.tatetian.hs.jobs;

import me.tatetian.hs.dataset.DataSet;
import me.tatetian.hs.dataset.DataSetFactory;
import me.tatetian.hs.io.FileUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConstructDataSet  extends Configured implements Tool  {
	@Override
	public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.out.println("dataset <mean> <sd> <num_records> <output>");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }	
		
		double mean 			= Double.parseDouble(args[0]), 
					 sd 				= Double.parseDouble(args[1]); 
		int 	 numRecords = Integer.parseInt(args[2]);
		Path	 outputPath = new Path(args[3]);
		
		DataSet dataSet = DataSetFactory.makeNormalDist(mean, sd);
		dataSet.setNumReords(numRecords);
		FSDataOutputStream out = FileUtil.createFile(outputPath, getConf());
		dataSet.dump(out);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ConstructDataSet(), args);
    System.exit(res);
  }	
}

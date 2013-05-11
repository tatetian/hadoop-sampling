package me.tatetian.hs.jobs;

import me.tatetian.hs.jobs.meancalculation.CalculateMean;
import me.tatetian.hs.jobs.meancalculation.CalculateMeanWithIndexedSampling;
import me.tatetian.hs.jobs.meancalculation.CalculateMeanWithNaiveSampling;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {
	public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
    	// e.g. hadoop jar hadoop-sampling-2.0.3-alpha.jar \
    	//								 dataset 100 80 1000000000 /hs/mean.data
      pgd.addClass("dataset", ConstructDataSet.class, 
                   "Construct an numeric dataset");
      // e.g. hadoop jar hadoop-sampling-2.0.3-alpha.jar \
      //								 index /hs/mean.data /hs/mean_index_output	
    	pgd.addClass("index", ConstructIndex.class, 
                   "Construct sampling index of a text data file");
    	// e.g. hadoop jar hadoop-sampling-2.0.3-alpha.jar \
    	//								 sample /hs/mean.data /hs/mean_sample_output
      pgd.addClass("sample", ExtractSample.class, 
                   "Extract sample from an indexed data file");
      // e.g. hadoop jar hadoop-sampling-2.0.3-alpha.jar \
      //								 mean0 /hs/mean.data /hs/mean_output
      pgd.addClass("mean0", CalculateMean.class, 
                   "Calculate mean of dataset");
      // e.g. hadoop jar hadoop-sampling-2.0.3-alpha.jar \
      //								 mean1 /hs/mean.data /hs/mean_output 0.01
      pgd.addClass("mean1", CalculateMeanWithNaiveSampling.class, 
                   "Calculate mean of dataset with naive sampling");
      // e.g. hadoop jar hadoop-sampling-2.0.3-alpha.jar \
      //								 mean2 /hs/mean.data /hs/mean_output 0.01
      pgd.addClass("mean2", CalculateMeanWithIndexedSampling.class, 
                   "Calculate mean of dataset with indexed sampling");
      exitCode = pgd.driver(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}

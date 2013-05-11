package me.tatetian.hs.jobs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestConstructDataSet {
	@Test
	public void test() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    
    ConstructDataSet job = new ConstructDataSet();
    job.setConf(conf);
    
    String mean = new String("100");
    String sd		= new String("80");
    String numRecords = new String("1000");
    String output = new String("tmp/test_construct_data_set.data.1000");
    job.run(new String[] {
    		mean, sd, numRecords, output
    });
	}
}

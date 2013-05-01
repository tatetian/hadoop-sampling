package me.tatetian.hs.jobs;

import me.tatetian.hs.jobs.ConstructIndex.ConstructIndexMapper;
import me.tatetian.hs.jobs.ConstructIndex.ConstructIndexReducer;
import me.tatetian.hs.jobs.ConstructIndex.InputSplitMeta;
import me.tatetian.hs.jobs.ConstructIndex.InputSplitSummary;

import org.apache.hadoop.io.IOUtils.NullOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestConstructIndex {
	
}
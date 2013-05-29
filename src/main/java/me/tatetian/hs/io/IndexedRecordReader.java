package me.tatetian.hs.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import me.tatetian.hs.index.Index;
import me.tatetian.hs.index.IndexFile;
import me.tatetian.hs.index.IndexMeta;
import me.tatetian.hs.index.IndexMetaFile;
import me.tatetian.hs.index.IndexUtil;
import me.tatetian.hs.sampler.SkipSampler;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class IndexedRecordReader extends RecordReader<LongWritable, Text> {
	private IndexedFileSplit.Reader splitReader;
  private FSDataInputStream dataIn;
  private LongWritable key = null;
  private Text value = null;

  private long start;
  private long end;
  private long pos;
  
  // Related to the current block
  private Index index;
  private int numRecords;
  private int nextRecord;
  
  private SkipSampler sampler = null;
  
  public IndexedRecordReader() {
  	this(true);
  }
  
  public IndexedRecordReader(boolean trimCR) {  	
  	this.key = new LongWritable(-1);
  	this.value = new Text();
  	this.value.setTrimCR(trimCR);
  }

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    if(!(genericSplit instanceof IndexedFileSplit))
    	throw new IllegalArgumentException("input split must be indexed");
  	
    // init split reader
  	IndexedFileSplit split = (IndexedFileSplit) genericSplit;
    Configuration conf = context.getConfiguration();
    splitReader = new IndexedFileSplit.Reader(split, conf);
    dataIn = splitReader.getDataStream();
    index  = splitReader.getIndex();
    
    // Init sampler
//    double samplingRatio = conf.getFloat(
//    								IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO, 
//    								IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO_DEFAULT);
//    sampler = new SkipSampler(samplingRatio);
//    
//    // Ready to read records
//    groupSize = conf.getInt(
//    								IOConfigKeys.HS_INDEXED_RECORD_READER_GROUP_SIZE, 
//    								IOConfigKeys.HS_INDEXED_RECORD_READER_GROUP_SIZE_DEFAULT);
//    groupRead = Integer.MAX_VALUE;
//    numBlocks = indexMeta.length;
//    nextRecord = numRecords = 0;
  }
  
  private boolean nextBlock() throws IOException {
  	// process next block in the split
  	if(!splitReader.nextBlock()) 
  		return false;
  		
  	// reset counter
  	pos = splitReader.position();
  	numRecords = index.size();
		nextRecord = 0;
  
  	return true;
  }
  
  public boolean nextKeyValue() throws IOException {
  	// find next chunk if needed
  	//if(groupRead > groupSize && !nextChunk())
  	//	return false;
  
  	// find next block if needed
  	if(nextRecord >= numRecords && !nextBlock() )
  		return false;	
  	
		// read this record in chunk
		key.set(pos);
  	int recordLen = index.get(nextRecord);
  	value.read(dataIn, recordLen);
  	
  	// update state
  	nextRecord ++;
  	pos += recordLen;
  	
  	return true;
  }
  
  private static final int SIZE_128KB = 128 * 1024;
  
//  private boolean nextChunk() throws IOException {
//  	int numChunksSkipped = sampler.next();
//  	int skip = 0;
//  	while(numChunksSkipped > 0) {
//  		int skippedBytes = 0;
//  		while(skippedBytes < groupSize) {
//	  		if(nextRecord >= numRecords && !nextBlock() )
//		  		return false;	
//  		
//  			skippedBytes += index.get(nextRecord);
//  			nextRecord ++;
//  		};
//  	
//  		skip += skippedBytes;
//  		numChunksSkipped --;
//  	}
//  
//  	pos += skip;
//  	
//  	while(skip > SIZE_128KB) {
//  		dataIn.skip(SIZE_128KB);
//  		skip -= SIZE_128KB;
//  	}
//  	if(skip > 0) dataIn.skip(skip);
//  	
//  	groupRead = 0;
//  	
//  	return true;
//  }
  
  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
  	if(end == start) 
  		return 0.0f;
  	else
  		return (pos - start) / (end - start);
  }
  
  public synchronized void close() throws IOException {
    if(splitReader != null)
    	splitReader.close();
  }
}

package me.tatetian.hs.io;

import java.io.IOException;
import java.nio.ByteBuffer;
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

  private long pos;
  
  // Related to the current block
  private Index index;
  private int nextRecord;
  private int numRecords;
  
  private int nextGroup;
  private int groupSize;
  private int numGroups;
  
  private int buffEndRecord;
  
  private int MAX_SEEKS;
  private float SAMPLING_RATIO;
  
  private final int BUFF_SIZE = 2 * 1024 * 1024; // 1M
  private byte[] buff = new byte[BUFF_SIZE]; 
  private int buffPos;
  
  private int numGroupsSkipped;
  
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
    SAMPLING_RATIO = conf.getFloat(
	    								IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO, 
	    								IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO_DEFAULT);
//    sampler = new SkipSampler(samplingRatio);
//    
//    // Ready to read records
    MAX_SEEKS = conf.getInt(
    								IOConfigKeys.HS_INDEXED_RECORD_READER_SEEKS, 
    								IOConfigKeys.HS_INDEXED_RECORD_READER_SEEKS_DEFAULT);
//    groupRead = Integer.MAX_VALUE;
//    numBlocks = indexMeta.length;
//    nextRecord = numRecords = 0;
  }
  
  private boolean nextBlock() throws IOException {
  	// process next block in the split
  	if(!splitReader.nextBlock()) 
  		return false;
  		
  	// reset counter 
  	numRecords = index.numRecords();
  	nextRecord = 0;  	
  	buffEndRecord = 0;
  	
  	int numSampledRecords = (int) Math.max(1, numRecords * SAMPLING_RATIO);
  	numGroups = Math.min(numSampledRecords, MAX_SEEKS) ;
  	groupSize = Math.max(1, numSampledRecords / numGroups);
  	nextGroup = 0;
  	
  	pos = splitReader.position();
  		
  	return true;
  }
  
  public boolean nextKeyValue() throws IOException {
  	if(isBufferDrain() && !fillBuffer()) 
  		return false;
  	
  	return readBuffer();
  }
  
  private boolean isBufferDrain() {
  	return nextRecord >= buffEndRecord;  
  }
  
  private boolean fillBuffer() throws IOException {
  	// load next block if needed
  	if(nextGroup >= numGroups && !nextBlock())
  		return false;
  	
  	// TODO: find next group
  	
  	// read data of the group
  	int fill = 0;
  	buffEndRecord = Math.min(nextRecord + groupSize, numRecords);
  	for(int i = nextRecord; i < buffEndRecord; i++) {
  		fill += index.get(i);
  	}  	
  	// TODO: get rid of the assumption that buffer is large enough to read data of any group
  	dataIn.readFully(buff, 0, fill);
  	buffPos = 0;  	
  	nextGroup ++;
  	
  	return true;
  }
  
  private boolean readBuffer() {
		// read from buffer
  	key.set(pos);
  	int recordLen = index.get(nextRecord);
	 	value.set(buff, buffPos, recordLen-1); // the last '\n' of record is omitted
  	// update state
	 	buffPos += recordLen;
  	nextRecord ++;
  	pos += recordLen;
  
  	return true;
  }
  
//  private boolean nextGroup() throws IOException {
//  	// find next block if needed
//  	if(nextGroup >= numGroups && !nextBlock() )
//  		return false;	
//  	
//  	
//  }
  
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
  	return splitReader.progress();
  }
  
  public synchronized void close() throws IOException {
    if(splitReader != null)
    	splitReader.close();
  }
}

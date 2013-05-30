package me.tatetian.hs.io;

import java.io.IOException;
import java.util.Random;

import me.tatetian.hs.index.Index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PreMapRecordReader extends RecordReader<LongWritable, Text> {
	private IndexedFileSplit.Reader splitReader;
  private FSDataInputStream dataIn;
  private LongWritable key = null;
  private Text value = null;

  private long pos;
  
  // Related to the current block
  private Index index;
  private int numRecords;
  
  private int MAX_SEEKS;
  private float SAMPLING_RATIO;
  
  private int numRecordsToSampled;
  private int numRecordsSampled;
  
  private Random random = new Random();
  
  public PreMapRecordReader() {
  	this(true);
  }
  
  public PreMapRecordReader(boolean trimCR) {  	
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
  	
  	numRecordsToSampled = (int) Math.max(1, numRecords * SAMPLING_RATIO);
  	numRecordsSampled = 0;
  	
  	pos = splitReader.position();
  		
  	return true;
  }
  
  public boolean nextKeyValue() throws IOException {
  	if(numRecordsSampled >= numRecordsToSampled && !nextBlock())
  		return false;
  	
  	int recordId = random.nextInt(numRecords);
  	long recordPos = 0;
  	for(int i = 0; i < recordId; i++) {
  		recordPos += index.get(i);
  	}
  	splitReader.seekInBlock(recordPos);
  	key.set(pos + recordPos);
  	value.read(dataIn, index.get(recordId) - 1);
  
  	numRecordsSampled ++;
  	return true;
  }
  
//  private boolean nextGroup() throws IOException {
//  	// find next block if needed
//  	if(nextGroup >= numGroups && !nextBlock() )
//  		return false;	
//  	
//  	
//  }
  
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


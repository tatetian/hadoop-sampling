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
  private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

  private FSDataInputStream in;
  private LongWritable key = null;
  private Text value = null;

  private long start;
  private long end;
  private long pos;
  
  private IndexMeta[] indexMeta;
  private int numBlocks;
  private int currentBlock;
  private Index currentIndex;
  private int currentBlockSize;
  private int currentRecord;
  private int chunkEnd;
  private IndexFile.Reader indexReader;
  
  private int chunkSize = 4096;
  private int chunkRead = 0; 
  
  // trim CR at end of record
  private boolean trimCR;
  
  private SkipSampler sampler = null;
  
  public IndexedRecordReader() {
  	this(true);
  }
  
  public IndexedRecordReader(boolean trimCR) {  	
  	this.key = new LongWritable(-1);
  	this.value = new Text();
  	this.trimCR = trimCR;
  	this.value.setTrimCR(trimCR);
  }

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    if(!(genericSplit instanceof IndexedFileSplit))
    	throw new IllegalArgumentException("input split must be indexed");
  	
  	IndexedFileSplit split = (IndexedFileSplit) genericSplit;
    Configuration conf = context.getConfiguration();
    
    // Open file
    Path dataFile = split.getPath();
    FileSystem fs = dataFile.getFileSystem(conf);
    in = fs.open(dataFile);
    start = split.getStart();
    end = start + split.getLength();
    
    pos = start;
    in.seek(pos);
    
    // Open index
    indexMeta = split.getIndexMeta();
    Path indexFile = IndexUtil.getIndexPath(dataFile);
    currentBlock = 0;
    indexReader = new IndexFile.Reader(conf, indexFile);
    long indexStart = indexMeta[currentBlock].indexBlockOffset;
    indexReader.seek(indexStart);
    currentIndex = Index.createIndex(conf);
    
    // Init sampler
    double samplingRatio = conf.getFloat(
    								IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO, 
    								IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO_DEFAULT);
    sampler = new SkipSampler(samplingRatio);
    
    // Ready to read records
    chunkSize = conf.getInt(
    								IOConfigKeys.HS_INDEXED_RECORD_READER_CHUNK_SIZE, 
    								IOConfigKeys.HS_INDEXED_RECORD_READER_CHUNK_SIZE_DEFAULT);
    chunkRead = Integer.MAX_VALUE;
    numBlocks = indexMeta.length;
    currentRecord = currentBlockSize = 0;
  }
  
  private boolean nextBlock() throws IOException {
  	if(currentBlock >= numBlocks) 
  		return false;
  
  	indexReader.next(currentIndex);
  	
  	currentBlockSize = currentIndex.size();
		currentRecord = 0;
  
  	currentBlock ++;
  	return true;
  }
  
  public boolean nextKeyValue() throws IOException {
  	// find next chunk if needed
  	if(chunkRead > chunkSize && !nextChunk())
  		return false;
  
  	// find next block if needed
  	if(currentRecord >= currentBlockSize && !nextBlock() )
  		return false;	
  	
		// read this record in chunk
		key.set(pos);
  	int recordLen = currentIndex.get(currentRecord);
  	value.read(in, recordLen);
  	
  	// update state
  	currentRecord ++;
  	pos += recordLen;
  	chunkRead += recordLen;
  	
  	return true;
  }
  
  private static final int SIZE_128KB = 128 * 1024;
  
  private boolean nextChunk() throws IOException {
  	int numChunksSkipped = sampler.next();
  	int skip = 0;
  	while(numChunksSkipped > 0) {
  		int skippedBytes = 0;
  		while(skippedBytes < chunkSize) {
	  		if(currentRecord >= currentBlockSize && !nextBlock() )
		  		return false;	
  		
  			skippedBytes += currentIndex.get(currentRecord);
  			currentRecord ++;
  		};
  	
  		skip += skippedBytes;
  		numChunksSkipped --;
  	}
  
  	pos += skip;
  	
  	while(skip > SIZE_128KB) {
  		in.skip(SIZE_128KB);
  		skip -= SIZE_128KB;
  	}
  	if(skip > 0) in.skip(skip);
  	
  	chunkRead = 0;
  	
  	return true;
  }
  
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
    if (in != null) 
      in.close();
    if (indexReader != null)
    	indexReader.close();
  }
}

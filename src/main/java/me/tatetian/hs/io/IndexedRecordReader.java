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
  private IndexFile.Reader indexReader;
  
  private boolean trimCR;
  
  private SkipSampler sampler = null;
  
  private boolean optimal = true;
  
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
    double samplingRatio = conf.getFloat("cps.sampling.ratio", 1.0f);
    sampler = new SkipSampler(samplingRatio);
    
    // Ready to read records
    numBlocks = indexMeta.length;
    currentRecord = currentBlockSize = 0;
  }
  
  private boolean readNextBlock() throws IOException {
  	if(currentBlock >= numBlocks) 
  		return false;
  
  	indexReader.next(currentIndex);
  	currentBlockSize = currentIndex.size();
  	if(optimal) {
  		in.seek(indexMeta[currentBlock].dataBlockOffset);
  		currentBlockSize *= sampler.getRatio();
  	}
  	currentRecord = 0;
  
  	currentBlock ++;
  	return true;
  }
  
  public boolean nextKeyValue() throws IOException {
  	int skipped = optimal ? 0 : sampler.next();
  	return nextKeyValue(skipped);
  }
  
  public boolean nextKeyValue(int skipped) throws IOException {
  	long skippedBytes = 0;
  	while(skipped > 0) {
	  	if(currentRecord >= currentBlockSize && !readNextBlock() )
	  		return false;
  		
	  	skippedBytes += currentIndex.get(currentRecord);
	  	
	  	currentRecord++;
	  	skipped--;
  	}
  	if(skippedBytes > 0) {
  		pos += skippedBytes;
  		if(pos >= end)
  			pos = end;
  		in.seek(pos);
  	}
  	
  	return _nextKeyValue();
  }
  
  private boolean _nextKeyValue() throws IOException {
		if(currentRecord >= currentBlockSize && !readNextBlock() )
  		return false;
  	
  	key.set(pos);
  	int recordLen = currentIndex.get(currentRecord);
  	value.read(in, recordLen);
  	
  	currentRecord ++;
  	pos += recordLen;
  	
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

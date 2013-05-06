package me.tatetian.hs.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import me.tatetian.hs.index.IndexMeta;
import me.tatetian.hs.index.IndexMetaFile;
import me.tatetian.hs.index.IndexUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class IndexedTextInputFormat extends FileInputFormat<LongWritable, Text> {
	@Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {		
		return getSplits(job, true);
	}
	
	
  public List<InputSplit> getSplits(JobContext job, boolean splitable) throws IOException {		
  	Configuration conf = job.getConfiguration();
		// Block merge ratio
		float samplingRatio = conf.getFloat("cps.sampling.ratio", 1.0f);
		if(samplingRatio <= 0 || samplingRatio > 1)
			throw new InvalidJobConfException("sampling ratio must be between 0 and 1");
		int numBlocksCombined = splitable ? (int) Math.floor(1/samplingRatio) :
																				Integer.MAX_VALUE ;
    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files 	= listStatus(job);
    List<IndexMeta>  metas 	= new ArrayList<IndexMeta>();
    for (FileStatus file: files) {
      long length = file.getLen();
      if (length > 0) {
      	Path inputPath = file.getPath();
      	Path indexMetaPath = IndexUtil.getIndexMetaPath(inputPath);
      	IndexMetaFile.Reader metaReader = new IndexMetaFile.Reader(conf, indexMetaPath);
        FileSystem fs = inputPath.getFileSystem(conf);
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
        
        // iterate blocks
				long splitStart = 0; long splitLen = 0;
				boolean hasBlocks = true;
				while(hasBlocks) {
					// combine blocks
					int blockCount = 0;
					while(blockCount < numBlocksCombined) {
						IndexMeta meta = new IndexMeta();
						hasBlocks = metaReader.next(meta);
						if(!hasBlocks) break;
						
						splitLen += meta.dataBlockLen;
						metas.add(meta);
						blockCount ++;
					}
					// add a split than spans across several blocks
					if(blockCount > 0) {
						int blkIndex = getBlockIndex(blkLocations, splitStart);
						InputSplit split = new IndexedFileSplit(
																	inputPath, splitStart, splitLen, 
																	blkLocations[blkIndex].getHosts(), 
																	(IndexMeta[]) metas.toArray(new IndexMeta[]{}));
						splits.add(split);
						
						splitStart += splitLen;
						splitLen = 0;
						metas.clear();
					}	
				}
      }
    }
    return splits;
  }	
  	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new IndexedRecordReader();
	}

}

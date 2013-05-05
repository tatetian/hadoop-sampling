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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class IndexedTextInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {		
		Configuration conf = job.getConfiguration();
		// Block merge ratio
		float samplingRatio = conf.getFloat("cps.sampling.ratio", 1.0f);
		int blockCombiningRatio = (int) Math.floor(samplingRatio);
    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    for (FileStatus file: files) {
      long length = file.getLen();
      if (length > 0) {
      	Path inputPath = file.getPath();
      	Path indexMetaPath = IndexUtil.getIndexMetaPath(inputPath);
      	IndexMetaFile.Reader metaReader = new IndexMetaFile.Reader(conf, indexMetaPath);
        FileSystem fs = inputPath.getFileSystem(conf);
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
        // combine blocks
				long start = 0; long len = 0;
				boolean readAllBlock = false;
				IndexMeta[] metas = new IndexMeta[blockCombiningRatio];
				while(!readAllBlock) {
					start += len;
					len = 0;
					int blockCount = 0;
					while(blockCount < blockCombiningRatio) {
						readAllBlock = metaReader.next(metas[blockCount]);
						if(readAllBlock) break;
						
						len += metas[blockCount].dataBlockLen;
						blockCount ++;
					}
					
					if(blockCount > 0) {
						int blkIndex = getBlockIndex(blkLocations, start);
						InputSplit split = new IndexedFileSplit(
																	inputPath, start, len, 
																	blkLocations[blkIndex].getHosts(), 
																	Arrays.copyOf(metas, blockCount));
						splits.add(split);
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

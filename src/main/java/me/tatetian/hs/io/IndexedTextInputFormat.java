package me.tatetian.hs.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	private static final String NO_HOSTS = "__NO_HOSTS__";
	
	@Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {		
		return getSplits(job, true);
	}
	
	
  public List<InputSplit> getSplits(JobContext job, boolean splitable) throws IOException {		
  	Configuration conf = job.getConfiguration();
		// Block merge ratio
		float samplingRatio = conf.getFloat(
											IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO, 
											IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO_DEFAULT);
		if(samplingRatio <= 0 || samplingRatio > 1)
			throw new InvalidJobConfException("sampling ratio must be between 0 and 1");
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

        // Find all data nodes
        Map<String, List<IndexMeta>> dataNodes = new HashMap<String, List<IndexMeta>>();
        dataNodes.put(NO_HOSTS, new ArrayList<IndexMeta>());
        for(BlockLocation bl: blkLocations) {
        	String[] hosts = bl.getHosts();
        	// if has hosts info
        	for(String host: hosts) {
        		if(!dataNodes.containsKey(host)) {
        			dataNodes.put(host, new ArrayList<IndexMeta>());
        		}
        	}
        }
        // Assign blocks to data nodes
        String[] strDataNodes = dataNodes.keySet().toArray(new String[]{});
        int numNodes = strDataNodes.length;
        int nodeId = 0;
        IndexMeta meta = new IndexMeta();
        while(metaReader.next(meta)) {
        	// find out hosts info of this block
        	long blkStart = meta.dataBlockOffset;
        	int blkIndex = getBlockIndex(blkLocations, blkStart);
        	BlockLocation blkLocation = blkLocations[blkIndex];
        	String[] hosts = blkLocation.getHosts();
        	String nodeChosen = NO_HOSTS;
        	if(hosts.length > 0) {
	        	// assign this block to a data node
	        	boolean foundHost = false;
	        	while(true) {
	        		for(String host : hosts) {
	        			if(host.equals(strDataNodes[nodeId])) {
	        				foundHost = true;
	        				break;
	        			}
	        		}        		
	        		nodeId = (nodeId + 1) % numNodes;        		
	        		
	        		if(foundHost)  break;
	        	}
	        	nodeChosen = strDataNodes[nodeId];
        	}
        	dataNodes.get(nodeChosen).add(meta);
        	meta = new IndexMeta();
        }
        // Build splits for each data node
        final int NUM_MAP_SLOTS = conf.getInt("mapreduce.tasktracker.map.tasks.maximum", 2);
        for(String node : strDataNodes) {
        	List<IndexMeta> blks = dataNodes.get(node);
        	int numBlks = blks.size();
        	int numEffectiveBlks = (int) (numBlks * samplingRatio);
        	int numSplits 	 = splitable ? Math.min(NUM_MAP_SLOTS, numEffectiveBlks) : 1;
        	int blksPerSplit = (int) Math.ceil( numBlks / numSplits );
        	int blkId = 0;
        	while(numBlks > 0) {
        		int numBlksSplit = Math.min(numBlks, blksPerSplit);
        		IndexMeta[] blksSplit = new IndexMeta[numBlksSplit];
        		long splitLen = 0;
        		for(int i = 0; i < numBlksSplit; i++) {
        			blksSplit[i] = blks.get(blkId + i);
        			splitLen += blksSplit[i].dataBlockLen;
        		}
        		long splitStart = blksSplit[0].dataBlockOffset;
        		InputSplit split = new IndexedFileSplit(
																	inputPath, splitStart, splitLen, 
																	node.equals(NO_HOSTS) ? null : new String[]{node},
																	blksSplit);
						splits.add(split);
						
        		numBlks -= numBlksSplit;
        		blkId   += numBlksSplit; 
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

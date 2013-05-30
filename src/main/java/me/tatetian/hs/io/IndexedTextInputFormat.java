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
		final float samplingRatio = conf.getFloat(
											IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO, 
											IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO_DEFAULT);
		if(samplingRatio <= 0 || samplingRatio > 1)
			throw new InvalidJobConfException("sampling ratio must be between 0 and 1");
    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files 	= listStatus(job);
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
        //dataNodes.put(NO_HOSTS, new ArrayList<IndexMeta>());
        int numNodes = strDataNodes.length;
        int nodeId = 0;
        IndexMeta meta = new IndexMeta();
        while(metaReader.next(meta)) {
        	// find out hosts info of this block
        	long blkStart = meta.dataBlockOffset;
        	int blkIndex = getBlockIndex(blkLocations, blkStart);
        	BlockLocation blkLocation = blkLocations[blkIndex];
        	String[] hosts = blkLocation.getHosts();
        	String nodeMissing = null;
        	// TODO: remove the assumption that there are 4 nodes
        	if(hosts.length > 0) {
        		nodeMissing = findMissingOne(hosts, strDataNodes);
        		assert(nodeMissing != null);
        	}
        	dataNodes.get(nodeMissing).add(meta);
        	meta = new IndexMeta();
        }
        // Build splits for each data node
        final int NUM_MAP_SLOTS = conf.getInt("mapreduce.tasktracker.map.tasks.maximum", 2);
        //strDataNodes = Arrays.copyOf(strDataNodes, strDataNodes.length + 1);
        //strDataNodes[strDataNodes.length - 1] = NO_HOSTS;
        for(String node : strDataNodes) {
        	List<IndexMeta> blks = dataNodes.get(node);
        	int numBlks = blks.size();
        	int numEffectiveBlks = (int) (numBlks * samplingRatio);
        	int numSplits 	 = splitable ? Math.max(NUM_MAP_SLOTS, numEffectiveBlks) : 1;
        	int blksPerSplit = (int) Math.ceil( (float)numBlks / numSplits );
        	int blkId = 0;
        	String[] hosts = removeMissingOne(strDataNodes, node);
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
																	hosts,
																	blksSplit);
        		System.out.println("hosts: " + Arrays.toString(hosts) + "; " + split);
						splits.add(split);
						
        		numBlks -= numBlksSplit;
        		blkId   += numBlksSplit; 
        	}
        }
      }
    }
    return splits;
  }	
  
  private String findMissingOne(String[] hosts, String[] allNodes) {
  	assert(hosts.length == 3);
  	assert(allNodes.length == 4);
  	
  	for(String node: allNodes) {
  		boolean found = false;
  		for(String host: hosts) {
  			if(host.equals(node)) {
  				found = true;
  				break;
  			}
  		}
  		if(!found) return node;
  	}
  	return null;
  }
  
  private String[] removeMissingOne(String[] allNodes, String missOne) {
  	String[] allNodesExceptOne = new String[allNodes.length - 1];
  	int i = 0, j = 0;
  	while(i < allNodes.length) {
  		if(!missOne.equals(allNodes[i])) {
  			allNodesExceptOne[j] = allNodes[i];
  			j ++;
  		}
  		i ++;
  	}
  	return allNodesExceptOne;
  }
  	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new IndexedRecordReader();
	}

}

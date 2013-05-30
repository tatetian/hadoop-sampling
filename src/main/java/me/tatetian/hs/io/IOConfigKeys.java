package me.tatetian.hs.io;

import org.apache.hadoop.hdfs.DFSConfigKeys;

public class IOConfigKeys extends DFSConfigKeys {
	public static final String HS_INDEXED_RECORD_READER_SEEKS = "hs.indexed-record-reader.seeks";
	public static final int		 HS_INDEXED_RECORD_READER_SEEKS_DEFAULT = Integer.MAX_VALUE;
	
	public static final String HS_INDEXED_RECORD_READER_SAMPLING_RATIO = "hs.indexed-record-reader.sampling-ratio";
	public static final float  HS_INDEXED_RECORD_READER_SAMPLING_RATIO_DEFAULT = 1.0f;
}

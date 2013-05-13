package me.tatetian.hs.io;

import org.apache.hadoop.hdfs.DFSConfigKeys;

public class IOConfigKeys extends DFSConfigKeys {
	public static final String HS_INDEXED_RECORD_READER_CHUNK_SIZE = "hs.indexed-record-reader.sample-size.min";
	public static final int		 HS_INDEXED_RECORD_READER_CHUNK_SIZE_DEFAULT = 0;
	
	public static final String HS_INDEXED_RECORD_READER_SAMPLING_RATIO = "hs.indexed-record-reader.sampling-ratio";
	public static final float  HS_INDEXED_RECORD_READER_SAMPLING_RATIO_DEFAULT = 1.0f;
}

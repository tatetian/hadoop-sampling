package me.tatetian.hs.index;

import java.io.IOException;
import java.util.Random;

import me.tatetian.hs.index.Index;
import me.tatetian.hs.index.IndexFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestIndexFile {
	private Configuration conf = new Configuration();
	private final Path indexFile = new Path("file:/tmp/hadoop-sampling/test.index");
	
	@Test
	public void testReadWrite() throws IOException {
		// Write		
		int numBlocks = 10;
		int numRecordsPerBlock = 1024;
		Index[] indexWritten = writeIndexes(indexFile, numBlocks, numRecordsPerBlock,
																				null);
		// Read
		IndexFile.Reader reader = null; 
		try {
		reader = new IndexFile.Reader(conf, indexFile);
			Index indexRead = new Index();
			int blockId = 0;
			while(reader.next(indexRead)) {
				Assert.assertEquals(indexWritten[blockId], indexRead);
				blockId ++;
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	@Test
	public void testRandomRead() throws IOException {
		// Write		
		int numBlocks = 10;
		int numRecordsPerBlock = 1024;		
		long[] indexStarts = new long[numBlocks];
		Index[] indexWritten = writeIndexes(indexFile, numBlocks, numRecordsPerBlock,
																				indexStarts);
		// Read
		IndexFile.Reader reader = null; 
		try {
			reader = new IndexFile.Reader(conf, indexFile);
			Index indexRead = new Index();
			for(int blockId = numBlocks - 1; blockId >= 0; blockId --) {
				reader.seek(indexStarts[blockId]);
				reader.next(indexRead);
				Assert.assertEquals(indexWritten[blockId], indexRead);
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	private Index[] writeIndexes(Path indexFile, 
															 int numBlocks, int numRecordsPerBlock,
															 long[] indexStarts) throws IOException {
		IndexFile.Writer writer = null;
		Index[] indexWritten = new Index[numBlocks];
		Random random = new Random();
		try {
			writer = new IndexFile.Writer(conf, indexFile);
			for(int blockId = 0; blockId < numBlocks; blockId++) {
				indexWritten[blockId] = new Index();
				int randomNumRecords = numRecordsPerBlock + random.nextInt(512);
				for(int recordId = 0; recordId < randomNumRecords; recordId++) {
					int randomRecordLen = 100 + random.nextInt(100);
					indexWritten[blockId].add(randomRecordLen);
				}
				if(indexStarts != null)
					indexStarts[blockId] = writer.getPosition();
				writer.append(indexWritten[blockId]);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		return indexWritten;
	}
}

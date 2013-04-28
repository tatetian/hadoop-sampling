package me.tatetian.cps.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexFile {
	@Test
	public void testReadWrite() throws IOException {
		Configuration conf = new Configuration();
		conf.setInt("cps.indexing.size.max", 1024 * 1024);
		Path indexFile = new Path("file:/tmp/hadoop-sampling/test.index");
		// Write
		IndexFile.Writer writer = null;
		int numBlocks = 10;
		int numRecordsPerBlock = 1024;
		Index[] indexWritten = new Index[numBlocks];
		try {
			writer = new IndexFile.Writer(conf, indexFile);
			for(int blockId = 0; blockId < numBlocks; blockId++) {
				indexWritten[blockId] = Index.createIndex(conf);
				for(int recordId = 0; recordId < numRecordsPerBlock + blockId * 128; recordId++) {
					indexWritten[blockId].add(recordId * 50);
				}
				writer.append(indexWritten[blockId]);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		// Read
		IndexFile.Reader reader = null; 
		try {
		reader = new IndexFile.Reader(conf, indexFile);
			Index indexRead = Index.createIndex(conf);
			int blockId = 0;
			while(reader.next(indexRead)) {
				Assert.assertEquals(indexWritten[blockId], indexRead);
				blockId ++;
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}

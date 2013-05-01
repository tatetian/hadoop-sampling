package me.tatetian.hs.index;

import java.io.IOException;

import me.tatetian.hs.index.IndexMeta;
import me.tatetian.hs.index.IndexMetaFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexMetaFile {
	@Test
	public void testReadWrite() throws IOException {
		Configuration conf = new Configuration();
		Path indexMetaFile = new Path("file:/tmp/hadoop-sampling/test.index.meta");
		// Write
		IndexMetaFile.Writer writer = null;
		int numBlocks = 1024;
		IndexMeta[] metaWritten = new IndexMeta[numBlocks];
		try {
			writer = new IndexMetaFile.Writer(conf, indexMetaFile);
			for(int blockId = 0; blockId < numBlocks; blockId++) {
				metaWritten[blockId] = new IndexMeta(blockId * 1024, 1024, 256, 
																						 blockId * 128, 128);
				writer.append(metaWritten[blockId]);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		// Read
		IndexMetaFile.Reader reader = null; 
		try {
		reader = new IndexMetaFile.Reader(conf, indexMetaFile);
			IndexMeta metaRead = new IndexMeta();
			int blockId = 0;
			while(reader.next(metaRead)) {
				Assert.assertEquals(metaWritten[blockId], metaRead);
				blockId ++;
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}

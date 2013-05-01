package me.tatetian.hs.index;

import junit.framework.Assert;

import me.tatetian.hs.index.IndexUtil;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestIndexUtil {
	@Test
	public void testPathGetter() {
		Path dataFile = new Path("file:/tmp/a.dat");
		Path indexFile = IndexUtil.getIndexPath(dataFile);
		Path metaFile = IndexUtil.getIndexMetaPath(dataFile);
		Assert.assertEquals("file:/tmp/.a.dat.index", indexFile.toString());
		Assert.assertEquals("file:/tmp/.a.dat.index.meta", metaFile.toString());
	}
}

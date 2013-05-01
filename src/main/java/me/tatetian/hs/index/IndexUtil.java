package me.tatetian.hs.index;

import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IndexUtil {
	public static Path getIndexPath(Path dataFile) {
		return new Path(dataFile.getParent(), new Path("." + dataFile.getName() + ".index"));
	}
	
	public static Path getIndexMetaPath(Path dataFile) {
		return new Path(dataFile.getParent(), new Path("." + dataFile.getName() + ".index.meta"));
	}
}

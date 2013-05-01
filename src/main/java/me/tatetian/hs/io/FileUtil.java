package me.tatetian.hs.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtil {
	public static FSDataOutputStream createFile(Path path, Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		int buffSize = 32 * 1024; // 32KB buffer
		FSDataOutputStream out = fs.create(path, true, buffSize);
		return out;
	}
	
	public static FSDataInputStream readFile(Path path, Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream in = fs.open(path);
		return in;
	}
	
	public static BufferedReader readTextFile(Path path, Configuration conf) throws IOException {
		FSDataInputStream in = readFile(path, conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		return reader;
	}
}

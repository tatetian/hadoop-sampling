package me.tatetian.hs.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
	
	public static boolean isContentIdentical(Path path1, Path path2, Configuration conf) throws IOException {
		FileSystem fs = path1.getFileSystem(conf);
		// Check length
		FileStatus fs1 = fs.getFileStatus(path1),
							 fs2 = fs.getFileStatus(path2);
		long len = fs1.getLen();
		if(len != fs2.getLen()) return false;
		System.out.println("!!!!!!!! len = " + len);
		// Check content
		FSDataInputStream in1 = fs.open(path1), 
											in2 = fs.open(path2);
		byte[] buff1 = new byte[1024], buff2 = new byte[1024];
		while(len > 0) {
			int read = len < 1024 ? (int)len : 1024;
			in1.readFully(buff1, 0, read);
			in2.readFully(buff2, 0, read);
			
			if(!Arrays.equals(buff1, buff2)) 
				return false;
			
			len -= read;
		}
		in1.close();
		in2.close();
		return true;
	}
}

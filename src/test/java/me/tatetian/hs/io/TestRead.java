package me.tatetian.hs.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Test how buffer size influence the performance of sequential read
 * */
public class TestRead {
	private File path = new File("tmp/zero_16G");
	
	public void test() throws IOException {
		long startTime, useTime; 
		int bufferMax = 8 * 1024 * 1024;
		for(int bufferSize = 256; bufferSize < bufferMax; bufferSize *= 2) {
			System.out.println("Reading file " + path + " with buffer size " + bufferSize + "...");
			startTime = System.currentTimeMillis();
			doRead(bufferSize);
			useTime = ( System.currentTimeMillis() - startTime ) / 1000;
			System.out.println("Elapsed " + useTime + " seconds");
		}
	}
	
	private void doRead(int bufferSize) throws IOException {
		FileInputStream in = new FileInputStream(path);
		byte[] buff = new byte[bufferSize];
		long bytes = 0; int read = 0;
		long megabytes = 100 * 1024 * 1024;
		do {
			read = in.read(buff);
			if(read > 0) bytes += read;
			if(bytes > megabytes) {
				System.out.print(".");
				bytes = 0;
			}
		} while( read > 0) ;
		in.close();
	} 
}

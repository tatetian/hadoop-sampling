package me.tatetian.cps.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Random;

import me.tatetian.cps.sampler.BernoulliSampler;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;

/**
 * <code>CascadingSampledDataOutputStream</code> implements the "sampling-while-loading" 
 * strategy of Cascading Persistence Sampling.
 * 
 * From a user's perspective, this <code>DataOutputStream</code> looks like writing 
 * to a normal file(local or distributed). But under the hood it actually write sampled 
 * records to multiple files in a cascading style. 
 * */
public class CascadingSampledDataOutputStream extends DataOutputStream implements Syncable {
	//===========================================================================
	// Factory methods
	//===========================================================================
	
	/**
	 * Create a new file to write(overwrite if exists)
	 * */
	public static CascadingSampledDataOutputStream create(FileSystem fs, Path path) throws IOException { 
		return new CascadingSampledDataOutputStream(fs, path, false);
	}
	
	/**
	 * Append an existing a file to write
	 * */
	public static CascadingSampledDataOutputStream append(FileSystem fs, Path path) throws IOException {
		return new CascadingSampledDataOutputStream(fs, path, true);
	}
	
	//===========================================================================
	// Public methods
	//===========================================================================
	
	/**
	 * Separate records
	 * 
	 * This function indicates the end of a record and beginning of a new record 
	 * so that the <code>OutputStream</code> can sample records accordingly. 
	 * */
	public void writeSeparator() throws IOException {
		write('\n');
		impl.newRecord();
	}
	
	//===========================================================================
	// Syncable inteface
	//===========================================================================

  @Override  // Syncable
  @Deprecated
  public void sync() throws IOException {
  	((CascadingSampledDataOutputStreamImpl)this.out).sync();
  }
  
  @Override  // Syncable
  public void hflush() throws IOException {
  	((CascadingSampledDataOutputStreamImpl)this.out).hflush();
  }
  
  @Override  // Syncable
  public void hsync() throws IOException {
  	((CascadingSampledDataOutputStreamImpl)this.out).hsync();
  }
	
	//===========================================================================
	// Private methods and classes
	//===========================================================================
  
  /**
   * Private constructor
   * */
	private CascadingSampledDataOutputStream(FileSystem fs, Path path, boolean appendMode) throws IOException {
		super(new CascadingSampledDataOutputStreamImpl(fs, path, appendMode));
		impl = (CascadingSampledDataOutputStreamImpl)this.out;
	}
	
	
	private CascadingSampledDataOutputStreamImpl impl = null;
	
	/**
	 * Private implementation class
	 * 
	 * This is where magic happens. One <code>FSDataOutputStream</code> is opened 
	 * to write sampled records an each level. For the last level, we maintains a buffer
	 * for sampled records and create a new cascading sampling file until the size of buffer exceeds 
	 * <code>dfs.block.size</code> approximately. 
	 * 
	 * A private OutputStream class is needed because the class <code>DataOutputStream</code>,
	 * which is the parent of <code>CascadingSampledDataOutputStream</code>, requires one and only one
	 * underlying <code>OutputStream</code> to do the job. Since we have multiple <code>FSDataOutputStream</code>
	 * instances(to write sampled records in a cascading fashion), we have to wrap them in a single 
	 * <code>OutputStream</code>. This class is the required underlying <code>OutputStream</code>.
	 * */
	private static class CascadingSampledDataOutputStreamImpl extends OutputStream implements Syncable {		
		//===========================================================================
		// Class variables
		//===========================================================================
		
		private FSDataOutputStream 		out = null;
		private OutputStream[] 				outs = null;
		private SamplableByteArrayOutputStream lastOut = null;
		
		private FileSystem fs = null;
		private Path path = null;
		
		private BernoulliSampler sampler = null;
		private int samplingLevels = 0;
		private int samplingLevelOfCurrentRecord = 0; 
		
		private int SAMPLE_RATIO = 0;
		private int MAX_SAMPLING_LEVEL = 0;
		private int MIN_SAMPLING_FILE_SIZE = 0;
		
		//===========================================================================
		// Public methods
		//===========================================================================
		
		/**
		 * Constructor
		 * */
		public CascadingSampledDataOutputStreamImpl(FileSystem fs, Path path, boolean appendMode) throws IOException {
			if(appendMode)
				throw new RuntimeException("To do the experiments, we only need write once. So append is not supported for now.");
			// Config
			SAMPLE_RATIO 						= fs.getConf().getInt("cps.sampling.ratio", 16);					// 1/16 by default
			MAX_SAMPLING_LEVEL 			= fs.getConf().getInt("cps.sampling.level.max", 8);				// 8 levels at most by default
			// TODO: check
			MIN_SAMPLING_FILE_SIZE 	= fs.getConf().getInt("dfs.block.size", 867108864) / 2;		// 32MB by default
			// Vars
			this.fs = fs;
			this.path = path;
			// Prepare streams
			try {
				int bufferSize = 4096 * 64;
				out 		= fs.create(path, true, bufferSize);																									// data file
				lastOut = new SamplableByteArrayOutputStream(MIN_SAMPLING_FILE_SIZE+1024);	// a buffer that is slightly larger than threshold
				outs		= new OutputStream[MAX_SAMPLING_LEVEL];															// cascading persistence sampling files
			}
			catch(IOException e) {
				closeAll();
				throw e;
			}
			// Prepare to sample
			sampler = new BernoulliSampler(SAMPLE_RATIO);
			samplingLevels = 1;
			outs[0] = lastOut; 
			// Ready to accept new record
			newRecord();
		}
		
		/**
		 * Write the content of the current record
		 * 
		 * If the sampler has decided to sample this record, then we will write to 
		 * the corresponding sampling files as well as the data file.
		 * */
    public void write(int b) throws IOException {
    	// Write to data file
    	out.write(b);
    	// Write to sample file
    	if(samplingLevelOfCurrentRecord > 0) {
    		int i = samplingLevelOfCurrentRecord;
    		while(--i >= 0) outs[i].write(b); 
    	}
    }
    
    @Override
    public void write(byte b[], int off, int len) throws IOException {
    	// Write to data file
    	out.write(b, off, len);
    	// Write to sample file
    	if(samplingLevelOfCurrentRecord > 0) {
    		int i = samplingLevelOfCurrentRecord;
    		while(--i >= 0) outs[i].write(b, off, len); 
    	}
    } 
    
    /**
     * Start a new record
     * 
     * Before writing a new record, the sampler decides whether to sample the record or not.
     * And if the buffer for the last sampling level exceeds a threshold, then 
     * a new sampling level is added.  
     * */
    public void newRecord() throws IOException {
    	// If we need to increase sampling level
    	if( samplingLevelOfCurrentRecord == samplingLevels && 
    		  lastOut.size() > MIN_SAMPLING_FILE_SIZE ) {
    		addNewSamplingLevel();
    	}
    	// Decide whether to sample the next record
    	samplingLevelOfCurrentRecord = 0;
    	while( samplingLevelOfCurrentRecord < samplingLevels && sampler.next() ) 
    		samplingLevelOfCurrentRecord ++;
    	// Tell sampling buffer the beginning of a new record
    	if( samplingLevelOfCurrentRecord == samplingLevels )
    		lastOut.newRecord();
    }

  	//===========================================================================
  	// Overridden methods
  	//===========================================================================
    @Override
    public void flush() throws IOException {
    	out.flush();
    	for(int i = 0; i < samplingLevels - 1; i++)
    		outs[i].flush();
    }
    @Override
    public void close() throws IOException {
			try {
			  flush();
			} 
			catch (IOException ignored) {
				System.out.println(ignored.toString());
			}
			closeAll();
	  }
    
  	//===========================================================================
  	// Syncable inteface
  	//===========================================================================
    @Override  // Syncable
    @Deprecated
    public void sync() throws IOException {
    	for(int i = 0; i < samplingLevels - 1; i++) {
    		if (outs[i] instanceof Syncable) {
    			((Syncable)outs[i]).sync();
    		}
    	}
    }
    @Override  // Syncable
    public void hflush() throws IOException {
    	for(int i = 0; i < samplingLevels - 1; i++) {
    		if (outs[i] instanceof Syncable) {
    			((Syncable)outs[i]).hflush();
	      } else {
	        outs[i].flush();
	      }
    	}
    }
    @Override  // Syncable
    public void hsync() throws IOException {
    	for(int i = 0; i < samplingLevels - 1; i++) {
	      if (outs[i] instanceof Syncable) {
	        ((Syncable)outs[i]).hsync();
	      } else {
	        outs[i].flush();
	      }
    	}
    }
    
  	//===========================================================================
  	// Private methods
  	//===========================================================================
    
    private void closeAll() throws IOException {
    	if(out != null) out.close();
    	for(int i = 0; i < samplingLevels; i++)
    		outs[i].close();
    }
    
    private void addNewSamplingLevel() throws IOException {
    	// Create and file a new sampling file
    	outs[samplingLevels-1] = fs.create(getSamplingFilePath(samplingLevels));
  		lastOut.writeTo(outs[samplingLevels-1]);
  		// Update sampling buffer of the last level
  		lastOut.updateBy(sampler);
  		// Update outputStream array
			outs[samplingLevels] 	 = lastOut;
			samplingLevels ++;
    }
    
    private Path getSamplingFilePath(int level) {
    	return path.suffix("." + Integer.toString(level));
    }
	}
}

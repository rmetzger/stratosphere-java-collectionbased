/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.io;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.core.fs.Path;


/**
 * The abstract base class for all output formats that are file based. Contains the logic to open/close the target
 * file streams.
 */
public abstract class FileOutputFormat<IT> implements OutputFormat<IT> {
	private static final long serialVersionUID = 1L;
	
	/**
	 * The LOG for logging messages in this class.
	 */
	private static final Log LOG = LogFactory.getLog(FileOutputFormat.class);
	
	/**
	 * The key under which the name of the target path is stored in the configuration. 
	 */
	public static final String FILE_PARAMETER_KEY = "stratosphere.output.file";
	
	/**
	 * The key under which the write mode is stored in the configuration
	 */
	public static final String WRITEMODE_PARAMETER_KEY = "stratosphere.output.writemode";

	/**
	 * Value keys for the write modes
	 */
	public static final String WRITEMODE_CREATE = "stratosphere.output.writemode.create";
	public static final String WRITEMODE_OVERWRITE = "stratosphere.output.writemode.overwrite";
	
	/**
     * The key under which the output directory mode parameter is stored in the configuration
     */
    public static final String OUT_DIRECTORY_PARAMETER_KEY = "stratosphere.output.directory";
    
    /**
     * Value keys for the output directory modes
     */
    public static final String OUT_DIRECTORY_ALWAYS = "stratosphere.output.directory.always";
    public static final String OUT_DIRECTORY_PARONLY = "stratosphere.output.directory.paronly";
    
	/**
	 * The config parameter for the opening timeout in milliseconds.
	 */
	public static final String OUTPUT_STREAM_OPEN_TIMEOUT_KEY = "stratosphere.output.file.timeout";
	
	/**
	 * The path of the file to be written.
	 */
	protected Path outputFilePath;
	
	/**
	 * The write mode of the output.	
	 */
	protected WriteMode writeMode;
	
	/**
	 * The output directory mode
	 */
	protected OutputDirectoryMode outDirMode;
	
	/**
	 * The stream to which the data is written;
	 */
	protected FSDataOutputStream stream;
	
	/**
	 * Stream opening timeout.
	 */
	private long openTimeout;

	// --------------------------------------------------------------------------------------------

	/**
	 * Defines the behavior for creating output directories. 
	 *
	 */
	public static enum OutputDirectoryMode {
		ALWAYS,			// A directory is always created, regardless of number of write tasks
		PARONLY			// A directory is only created for parallel output tasks, i.e., number of output tasks > 1.
						// If number of output tasks = 1, the output is written to a single file.
	}
	
	@Override
	public void configure(Configuration parameters) {
		// get the file parameter
		String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath == null) {
			throw new IllegalArgumentException("Configuration file FileOutputFormat does not contain the file path.");
		}
		
		try {
			this.outputFilePath = new Path(filePath);
		}
		catch (RuntimeException rex) {
			throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage()); 
		}
		
		// get the write mode parameter
		String writeModeParam = parameters.getString(WRITEMODE_PARAMETER_KEY, WRITEMODE_OVERWRITE);
		if(writeModeParam.equals(WRITEMODE_OVERWRITE)) {
			this.writeMode = WriteMode.OVERWRITE;
		} else if(writeModeParam.equals(WRITEMODE_CREATE)) {
			this.writeMode = WriteMode.CREATE;
		} else {
			throw new RuntimeException("Invalid write mode configuration: "+writeModeParam);
		}
		
		// get the output directory parameter
		String outDirParam = parameters.getString(OUT_DIRECTORY_PARAMETER_KEY, OUT_DIRECTORY_PARONLY);
		if(outDirParam.equals(OUT_DIRECTORY_ALWAYS)) {
			this.outDirMode = OutputDirectoryMode.ALWAYS;
		} else if(outDirParam.equals(OUT_DIRECTORY_PARONLY)) {
			this.outDirMode = OutputDirectoryMode.PARONLY;
		} else {
			throw new RuntimeException("Invalid output directory mode configuration: "+outDirParam);
		}
		
		// get timeout for stream opening
		this.openTimeout = parameters.getLong(OUTPUT_STREAM_OPEN_TIMEOUT_KEY, FileInputFormat.DEFAULT_OPENING_TIMEOUT);
		if (this.openTimeout < 0) {
			this.openTimeout = FileInputFormat.DEFAULT_OPENING_TIMEOUT;
			if (LOG.isWarnEnabled())
				LOG.warn("Ignoring invalid parameter for stream opening timeout (requires a positive value or zero=infinite): " + this.openTimeout);
		} else if (this.openTimeout == 0) {
			this.openTimeout = Long.MAX_VALUE;
		}
	}



	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		// obtain FSDataOutputStream asynchronously, since HDFS client can not handle InterruptedExceptions
		OutputPathOpenThread opot = new OutputPathOpenThread(this, (taskNumber + 1), numTasks);
		opot.start();
		
		try {
			// get FSDataOutputStream
			this.stream = opot.waitForCompletion();
		}
		catch (Exception e) {
			throw new RuntimeException("Stream to output file could not be opened: " + e.getMessage(), e);
		}
	}



	@Override
	public void close() throws IOException {
		final FSDataOutputStream s = this.stream;
		if (s != null) {
			this.stream = null;
			s.close();
		}
	}
	
	public Path getOutputFilePath() {
		return this.outputFilePath;
	}
	
	public WriteMode getWriteMode() {
		return this.writeMode;
	}
	
	public OutputDirectoryMode getOutDirMode() {
		return this.outDirMode;
	}
	
	public long getOpenTimeout() {
		return this.openTimeout;
	}
	
	// ============================================================================================
	
	private static final class OutputPathOpenThread extends Thread {
		
		private final Path path;
		
		private final int taskIndex;
		
		private final int numTasks;
		
		private final WriteMode writeMode;
		
		private final OutputDirectoryMode outDirMode;
		
		private final long timeoutMillies;
		
		private volatile FSDataOutputStream fdos;

		private volatile Throwable error;
		
		private volatile boolean aborted;

		
		public OutputPathOpenThread(FileOutputFormat<?> fof, int taskIndex, int numTasks) {
			this.path = fof.getOutputFilePath();
			this.writeMode = fof.getWriteMode();
			this.outDirMode = fof.getOutDirMode();
			this.timeoutMillies = fof.getOpenTimeout();
			this.taskIndex = taskIndex;
			this.numTasks = numTasks;
		}

		@Override
		public void run() {
			
			try {
				Path p = this.path;
				final FileSystem fs = p.getFileSystem();

				// initialize output path. 
				if(this.numTasks == 1 && outDirMode == OutputDirectoryMode.PARONLY) {
					// output is not written in parallel and should go to a single file
					
					if(!fs.isDistributedFS()) {
						// prepare local output path
						// checks for write mode and removes existing files in case of OVERWRITE mode
						if(!fs.initOutPathLocalFS(p, writeMode, false)) {
							// output preparation failed! Cancel task.
							throw new IOException("Output path could not be initialized. Canceling task.");
						}
					}
					
				} else if(this.numTasks > 1 || outDirMode == OutputDirectoryMode.ALWAYS) {
					// output is written in parallel into a directory or should always be written to a directory
					
					if(!fs.isDistributedFS()) {
						// File system is not distributed.
						// We need to prepare the output path on each executing node.
						if(!fs.initOutPathLocalFS(p, writeMode, true)) {
							// output preparation failed! Cancel task.
							throw new IOException("Output directory could not be created. Canceling task.");
						}
					}
					
					// Suffix the path with the parallel instance index
					p = p.suffix("/" + this.taskIndex);
					
				} else {
					// invalid number of subtasks (<= 0)
					throw new IllegalArgumentException("Invalid number of subtasks. Canceling task.");
				}
					
				// create output file
				switch(writeMode) {
				case CREATE: 
					this.fdos = fs.create(p, false);
					break;
				case OVERWRITE:
					this.fdos = fs.create(p, true);
					break;
				default:
					throw new IllegalArgumentException("Invalid write mode: "+writeMode);
				}
				
				// check for canceling and close the stream in that case, because no one will obtain it
				if (this.aborted) {
					final FSDataOutputStream f = this.fdos;
					this.fdos = null;
					f.close();
				}
			}
			catch (Throwable t) {
				this.error = t;
			}
		}
		
		public FSDataOutputStream waitForCompletion() throws Exception {
			final long start = System.currentTimeMillis();
			long remaining = this.timeoutMillies;
			
			do {
				try {
					this.join(remaining);
				} catch (InterruptedException iex) {
					// we were canceled, so abort the procedure
					abortWait();
					throw iex;
				}
			}
			while (this.error == null && this.fdos == null &&
					(remaining = this.timeoutMillies + start - System.currentTimeMillis()) > 0);
			
			if (this.error != null) {
				throw new IOException("Opening the file output stream failed" +
					(this.error.getMessage() == null ? "." : ": " + this.error.getMessage()), this.error);
			}
			
			if (this.fdos != null) {
				return this.fdos;
			} else {
				// double-check that the stream has not been set by now. we don't know here whether
				// a) the opener thread recognized the canceling and closed the stream
				// b) the flag was set such that the stream did not see it and we have a valid stream
				// In any case, close the stream and throw an exception.
				abortWait();
				
				final boolean stillAlive = this.isAlive();
				final StringBuilder bld = new StringBuilder(256);
				for (StackTraceElement e : this.getStackTrace()) {
					bld.append("\tat ").append(e.toString()).append('\n');
				}
				throw new IOException("Output opening request timed out. Opener was " + (stillAlive ? "" : "NOT ") + 
					" alive. Stack:\n" + bld.toString());
			}
		}
		
		/**
		 * Double checked procedure setting the abort flag and closing the stream.
		 */
		private final void abortWait() {
			this.aborted = true;
			final FSDataOutputStream outStream = this.fdos;
			this.fdos = null;
			if (outStream != null) {
				try {
					outStream.close();
				} catch (Throwable t) {}
			}
		}
	}
	
	// ============================================================================================
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureFileFormat(FileDataSink target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * A builder used to set parameters to the output format's configuration in a fluent way.
	 */
	public static abstract class AbstractConfigBuilder<T> {
		
		/**
		 * The configuration into which the parameters will be written.
		 */
		protected final Configuration config;
		
		// --------------------------------------------------------------------
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected AbstractConfigBuilder(Configuration targetConfig) {
			this.config = targetConfig;
		}
		
		// --------------------------------------------------------------------
		
		/**
		 * Sets the timeout after which the output format will abort the opening of the output stream,
		 * if the stream has not responded until then.
		 * 
		 * @param timeoutInMillies The timeout, in milliseconds, or <code>0</code> for infinite.
		 * @return The builder itself.
		 */
		public T openingTimeout(int timeoutInMillies) {
			this.config.setLong(OUTPUT_STREAM_OPEN_TIMEOUT_KEY, timeoutInMillies);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
	}
	
	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	public static class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder> {
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected ConfigBuilder(Configuration targetConfig) {
			super(targetConfig);
		}
	}
}
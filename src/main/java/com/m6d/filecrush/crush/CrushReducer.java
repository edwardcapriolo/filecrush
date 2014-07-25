/*
   Copyright 2011 m6d.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.m6d.filecrush.crush;

import static java.lang.String.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

@SuppressWarnings("deprecation")
public class CrushReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	private final Text valueOut = new Text();

	/**
	 * Internal counter for the number of input groups processed. Used to report status.
	 */
	private int fileNum;

	/**
	 * The number of source files that have been crushed.
	 */
	private int recordNumber;

	/**
	 * Report status when after processing this number of files.
	 */
	private int reportRecordNumber = 100;

	private int taskNum;

	private long timestamp;

	private JobConf job;

	private FileSystem fs;

	/**
	 * Matched against dir names to calculate the crush output file name.
	 */
	private List<Matcher> inputRegexList;

	/**
	 * Used with corresponding element in {@link #inputRegexList} to calculate the crush ouput file name.
	 */
	private List<String> outputReplacementList;

	/**
	 * Input formats that correspond with {@link #inputRegexList}.
	 */
	private List<Class<?>> inFormatClsList;

	/**
	 * Output formats that correspond with {@link #inputRegexList}.
	 */
	private List<Class<?>> outFormatClsList;

	/**
	 * Used to substitute values into placeholders.
	 */
	private Map<String, String> placeHolderToValue = new HashMap<String, String>(3);

	/**
	 * Used to locate placeholders in the replacement strings.
	 */
	private Matcher placeholderMatcher = Pattern.compile("\\$\\{([a-zA-Z]([a-zA-Z\\.]*))\\}").matcher("dummy");

	/**
	 * Path to the output dir of the job. Used to compute the final output file names for the crush files, which are the values in
	 * the reducer output.
	 */
	private String outDirPath;

	@Override
	public void configure(JobConf job) {
		super.configure(job);

		this.job = job;

		taskNum = Integer.parseInt(job.get("mapred.tip.id").replaceFirst(".+_(\\d+)", "$1"));
		timestamp = Long.parseLong(job.get("crush.timestamp"));

		outDirPath = job.get("mapred.output.dir");

		if (null == outDirPath || outDirPath.isEmpty()) {
			throw new IllegalArgumentException("mapred.output.dir has no value");
		}

		/*
		 * The files we write should be rooted in the "crush" subdir of the output directory to distinguish them from the files
		 * created by the collector.
		 */
		Path outDirP = new Path(outDirPath + "/crush");
		outDirPath = outDirP.toUri().getPath();

		/*
		 * Configure the regular expressions and replacements we use to convert dir names to crush output file names. Also get the
		 * directory data formats.
		 */
		int numSpecs = job.getInt("crush.num.specs", 0);

		if (numSpecs <= 0) {
			throw new IllegalArgumentException("Number of regular expressions must be zero or greater: " + numSpecs);
		}

		readCrushSpecs(numSpecs);

		placeHolderToValue.put("crush.task.num", Integer.toString(taskNum));
		placeHolderToValue.put("crush.timestamp", job.get("crush.timestamp"));

		try {
			fs = outDirP.getFileSystem(job);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Populates the following fields with non-default values from the configuration.
	 *
	 * <ul>
	 * <li><{@link #inputRegexList}/li>
	 * <li><{@link #outputReplacementList}/li>
	 * <li><{@link #inFormatClsList}/li>
	 * <li><{@link #outFormatClsList}/li>
	 * </ul>
	 */
	private void readCrushSpecs(int numSpecs) {
		inputRegexList = new ArrayList<Matcher>(numSpecs);
		outputReplacementList = new ArrayList<String>(numSpecs);
		inFormatClsList = new ArrayList<Class<?>>(numSpecs);
		outFormatClsList = new ArrayList<Class<?>>(numSpecs);

		for (int i = 0; i < numSpecs; i++) {
			String key;
			String value;

			/*
			 * Regex.
			 */
			key = format("crush.%d.regex", i);
			value = job.get(key);

			if (null == value || value.isEmpty()) {
				throw new IllegalArgumentException("No input regex: " + key);
			}

			inputRegexList.add(Pattern.compile(value).matcher("dummy"));

			/*
			 * Replacement for regex.
			 */
			key = format("crush.%d.regex.replacement", i);
			value = job.get(key);

			if (null == value || value.isEmpty()) {
				throw new IllegalArgumentException("No output replacement: " + key);
			}

			outputReplacementList.add(value);

			/*
			 * Input format
			 */
			key = format("crush.%d.input.format", i);
			value = job.get(key);

			if (null == value || value.isEmpty()) {
				throw new IllegalArgumentException("No input format: " + key);
			}

			try {
				Class<?> inFormatCls;

				if (value.equals(TextInputFormat.class.getName())) {
					inFormatCls = KeyValuePreservingTextInputFormat.class;
				} else {
				 inFormatCls = Class.forName(value);

					if (!FileInputFormat.class.isAssignableFrom(inFormatCls)) {
						throw new IllegalArgumentException(format("Not a file input format: %s=%s", key, value));
					}
				}

				inFormatClsList.add(inFormatCls);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(format("Not a valid class: %s=%s", key, value));
			}

			/*
			 * Output format.
			 */
			key = format("crush.%d.output.format", i);
			value = job.get(key);

			if (null == value || value.isEmpty()) {
				throw new IllegalArgumentException("No output format: " + key);
			}

			try {
				Class<?> outFormatCls = Class.forName(value);

				if (!OutputFormat.class.isAssignableFrom(outFormatCls)) {
					throw new IllegalArgumentException(format("Not an output format: %s=%s", key, value));
				}

				outFormatClsList.add(outFormatCls);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(format("Not a valid class: %s=%s", key, value));
			}
		}
	}

	@Override
	public void reduce(Text bucketId, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
		String bucket = bucketId.toString();

		String dirName = bucket.substring(0, bucket.lastIndexOf('-'));

		int idx = findMatcher(dirName);

		String outputFileName = calculateOutputFile(idx, dirName);

		/*
		 * Don't need to separate the paths because the output file name is already absolute.
		 */
		valueOut.set(outDirPath + outputFileName);

		LOG.info(format("Crushing bucket '%s' to file '%s'", bucket, outputFileName));

		/*
		 * Strip the leading slash to make the path relative. the output format will relativize it to the task attempt work dir.
		 */
		RecordWriter<Object, Object> sink = null;
		Exception rootCause = null;

		Object key = null;
		Object value = null;

		try {
			while (null == rootCause && values.hasNext()) {
				Text srcFile = values.next();
				Path inputPath = new Path(srcFile.toString());

				RecordReader<Object, Object> reader = createRecordReader(idx, inputPath, reporter);

				try {
					if (null == key) {
						key = reader.createKey();
						value = reader.createValue();

						/*
						 * Set the key and value class in the conf, which the output format uses to get type information.
						 */
						job.setOutputKeyClass(key.getClass());
						job.setOutputValueClass(value.getClass());

						/*
						 * Output file name is absolute so we can just add it to the crush prefix.
						 */
						sink = createRecordWriter(idx, "crush" + outputFileName);
					} else {

						Class<?> other = reader.createKey().getClass();

						if (!(key.getClass().equals(other))) {
							throw new IllegalArgumentException(format("Heterogeneous keys detected in %s: %s !- %s", inputPath, key.getClass(), other));
						}

						other = reader.createValue().getClass();

						if (!value.getClass().equals(other)) {
							throw new IllegalArgumentException(format("Heterogeneous values detected in %s: %s !- %s", inputPath, value.getClass(), other));
						}
					}

					while (reader.next(key, value)) {
						sink.write(key, value);
						reporter.incrCounter(ReducerCounter.RECORDS_CRUSHED, 1);
					}
				} catch (Exception e) {
					rootCause = e;
				} finally {
					try {
						reader.close();
					} catch (Exception e) {
						if (null == rootCause) {
							rootCause = e;
						} else {
							LOG.debug("Swallowing exception on close of " + inputPath, e);
						}
					}
				}

				/*
				 * Output of the reducer is the source file => crushed file (in the final output dir, no the task attempt work dir.
				 */
				collector.collect(srcFile, valueOut);
				reporter.incrCounter(ReducerCounter.FILES_CRUSHED, 1);

				recordNumber++;

				if (reportRecordNumber == recordNumber) {
					reportRecordNumber += reportRecordNumber;

					reporter.setStatus(format("Processed %,d files %s : %s", recordNumber, bucket, inputPath));
				}
			}
		} catch (Exception e) {
			rootCause = e;
		} finally {
			if (null != sink) {
				try {
					sink.close(reporter);
				} catch (Exception e) {
					if (null == rootCause) {
						rootCause = e;
					} else {
						LOG.error("Swallowing exception on close of " + outputFileName, e);
					}
				}
			}

			/*
			 * Let the exception bubble up with a minimum of wrapping.
			 */
			if (null != rootCause) {
				if (rootCause instanceof RuntimeException) {
					throw (RuntimeException) rootCause;
				}

				if (rootCause instanceof IOException) {
					throw (IOException) rootCause;
				}

				throw new RuntimeException(rootCause);
			}
		}
	}

	/**
	 * Returns a record writer that creates files in the task attempt work directory. Path must be relative!
	 */
	@SuppressWarnings("unchecked")
	private RecordWriter<Object, Object> createRecordWriter(int idx, String path) throws IOException {
		Class<? extends OutputFormat<?, ?>> cls = (Class<? extends OutputFormat<?, ?>>) outFormatClsList.get(idx);

		try {
			OutputFormat<Object, Object> format = (OutputFormat<Object, Object>) cls.newInstance();

			return format.getRecordWriter(fs, job, path, null);
		} catch (RuntimeException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private RecordReader<Object, Object> createRecordReader(int idx, Path inputPath, Reporter reporter) throws IOException {

		LOG.info(format("Opening '%s'", inputPath));

		Class<? extends FileInputFormat<?, ?>> cls = (Class<? extends FileInputFormat<?, ?>>) inFormatClsList.get(idx);

		try {
			FileInputFormat.setInputPaths(job, inputPath);

			FileInputFormat<?, ?> instance = cls.newInstance();

			if (instance instanceof JobConfigurable) {
				((JobConfigurable) instance).configure(job);
			}

			InputSplit[] splits = instance.getSplits(job, 1);

			if (1 != splits.length) {
				throw new IllegalArgumentException("Could not get input splits: " + inputPath);
			}

			return (RecordReader<Object, Object>) instance.getRecordReader(splits[0], job, reporter);
		} catch (RuntimeException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Converts the name of a directory to a path to the crush output file using the specs at the given index. The path will the
	 * directory and file name separated by a slash /. Performs placeholder substitution on the corresponding replacement string in
	 * {@link #outputReplacementList}. The final replacement string is then used to form the final path.
	 */
	String calculateOutputFile(int idx, String srcDir) {

		StringBuffer sb = new StringBuffer(srcDir);
		sb.append("/");

		String replacement = outputReplacementList.get(idx);

		placeHolderToValue.put("crush.file.num", Integer.toString(fileNum++));

		placeholderMatcher.reset(replacement);

		while (placeholderMatcher.find()) {
			String key = placeholderMatcher.group(1);

			String value = placeHolderToValue.get(key);

			if (null == value) {
				throw new IllegalArgumentException("No value for key: " + key);
			}

			placeholderMatcher.appendReplacement(sb, value);
		}

		placeholderMatcher.appendTail(sb);

		Matcher matcher = inputRegexList.get(idx);
		matcher.reset(srcDir);

		String finalOutputName = matcher.replaceAll(sb.toString());

		return finalOutputName;
	}

	/**
	 * Returns the index into {@link #inputRegexList} of first pattern that matches the argument.
	 */
	int findMatcher(String dir) {

		String outputNameWithPlaceholders = null;

		for (int i = 0; i < inputRegexList.size() && outputNameWithPlaceholders == null; i++) {
			Matcher matcher = inputRegexList.get(i);

			matcher.reset(dir);

			if (matcher.matches()) {
				return i;
			}
		}

		throw new IllegalArgumentException("No matching input regex: " + dir);
	}

	int getTaskNum() {
		return taskNum;
	}

	long getTimestamp() {
		return timestamp;
	}

	List<String> getInputRegexList() {
		ArrayList<String> list = new ArrayList<String>(inputRegexList.size());

		for (Matcher matcher : inputRegexList) {
			list.add(matcher.pattern().pattern());
		}

		return list;
	}

	List<String> getOutputReplacementList() {
		return new ArrayList<String>(outputReplacementList);
	}

	List<Class<?>> getInputFormatList() {
		return new ArrayList<Class<?>>(inFormatClsList);
	}

	List<Class<?>> getOutputFormatList() {
		return new ArrayList<Class<?>>(outFormatClsList);
	}

	private static final Log LOG = LogFactory.getLog(CrushReducer.class);
}

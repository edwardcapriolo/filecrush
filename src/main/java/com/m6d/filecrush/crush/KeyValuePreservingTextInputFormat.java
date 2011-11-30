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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * {@link TextInputFormat} creates keys of {@link LongWritable} offsets and {@link Text} values, which contain the line. For file
 * crushing, we need to preserve the keys and values as they appear in the file, which means we must discard the byte offsets and
 * divide the value into the original key and value pairs.
 */
@SuppressWarnings("deprecation")
public class KeyValuePreservingTextInputFormat extends FileInputFormat<Text, Text> {

	private TextInputFormat delegate;

  public void configure(JobConf conf) {
  	delegate = new TextInputFormat();
  	delegate.configure(conf);
  }

  @Override
	protected boolean isSplitable(FileSystem fs, Path file) {
  	/*
  	 * Return false because the reducer opens the file from beginning to end.
  	 */
    return false;
  }

  @Override
	public RecordReader<Text, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {

    reporter.setStatus(genericSplit.toString());

    return new KeyValuePreservingRecordReader(new LineRecordReader(job, (FileSplit) genericSplit));
  }

  static class KeyValuePreservingRecordReader implements RecordReader<Text, Text> {

  	private final RecordReader<LongWritable, Text> delegate;

  	private final LongWritable delKey = new LongWritable();

  	private final Text delValue = new Text();

		public KeyValuePreservingRecordReader(RecordReader<LongWritable, Text> delegate) {
			super();

			this.delegate = delegate;
		}

		@Override
		public Text createKey() {
			return new Text();
		}

		@Override
		public Text createValue() {
			return delegate.createValue();
		}

		@Override
		public long getPos() throws IOException {
			return delegate.getPos();
		}

		@Override
		public void close() throws IOException {
			delegate.close();
		}

		@Override
		public float getProgress() throws IOException {
			return delegate.getProgress();
		}

		@Override
		public boolean next(Text key, Text value) throws IOException {
			boolean next = delegate.next(delKey, delValue);

			if (next) {
				int first = delValue.find("\t");

				if (first >= 0) {
					key.set(delValue.getBytes(), 0, first);

					if (delValue.getLength() > first) {
						value.set(delValue.getBytes(), first + 1, delValue.getLength() - first - 1);
					} else {
						value.clear();
					}
				} else {
					key.set(delValue);
				}
			}

			return next;
		}
  }
}

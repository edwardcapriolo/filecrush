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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class CountersInputFormat extends FileInputFormat<Counters, NullWritable> {

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	public RecordReader<Counters, NullWritable> getRecordReader(InputSplit inputSplit, JobConf jobconf, Reporter reporter)
			throws IOException {

		if (!(inputSplit instanceof FileSplit)) {
			throw new AssertionError();
		}

		FileSplit fSplit = (FileSplit) inputSplit;

		Path path = fSplit.getPath();
		long length = fSplit.getLength();

		FileSystem fs = FileSystem.get(jobconf);

		FSDataInputStream is = fs.open(path);

		return new CountersReader(is, length);
	}

	private static class CountersReader implements RecordReader<Counters, NullWritable> {

		private final FSDataInputStream in;

		private final long length;

		public CountersReader(FSDataInputStream in, long length) {
			super();

			this.in = in;
			this.length = length;
		}

		@Override
		public Counters createKey() {
			return new Counters();
		}

		@Override
		public NullWritable createValue() {
			return NullWritable.get();
		}

		@Override
		public long getPos() throws IOException {
			return in.getPos();
		}

		@Override
		public float getProgress() throws IOException {
			float percent = ((float) length) / in.getPos();

			return percent;
		}

		@Override
		public boolean next(Counters key, NullWritable value) throws IOException {
			if (0 == in.getPos()) {
				key.readFields(in);

				return true;
			}

			return false;
		}

		@Override
		public void close() throws IOException {
			in.close();
		}
	}
}

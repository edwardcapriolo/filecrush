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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.m6d.filecrush.crush.CrushPartitioner;

@SuppressWarnings("deprecation")
public class CrushPartitionerTest {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private JobConf job;

	private FileSystem fs;

	private Path partitionMap;

	private CrushPartitioner partitioner;

	@Before
	public void setupPartitionMap() throws IOException {
		job = new JobConf(false);

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.set("crush.partition.map", tmp.getRoot().getAbsolutePath() + "/partition-map");

		fs = FileSystem.get(job);

		partitionMap = new Path(tmp.getRoot().getAbsolutePath(), "partition-map");

		partitioner = new CrushPartitioner();
	}

	@Test
	public void partition() throws IOException {

		Writer writer = SequenceFile.createWriter(fs, job, partitionMap, Text.class, IntWritable.class);

		Text key = new Text();
		IntWritable partNum = new IntWritable();

		key.set("bucket-1");
		partNum.set(0);
		writer.append(key, partNum);

		key.set("bucket-2");
		partNum.set(0);
		writer.append(key, partNum);

		key.set("bucket-3");
		partNum.set(1);
		writer.append(key, partNum);

		key.set("bucket-4");
		partNum.set(2);
		writer.append(key, partNum);

		key.set("bucket-5");
		partNum.set(2);
		writer.append(key, partNum);

		key.set("bucket-6");
		partNum.set(2);
		writer.append(key, partNum);

		writer.close();

		job.setNumReduceTasks(3);


		partitioner.configure(job);


		Text fileName = new Text();

		key.set("bucket-1");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(0));
		}


		key.set("bucket-2");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(0));
		}


		key.set("bucket-3");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(1));
		}


		key.set("bucket-4");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(2));
		}


		key.set("bucket-5");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(2));
		}


		key.set("bucket-6");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(2));
		}
	}


	@Test
	public void partitionWithFewerPartitionsThanReduceTasks() throws IOException {

		Writer writer = SequenceFile.createWriter(fs, job, partitionMap, Text.class, IntWritable.class);

		Text key = new Text();
		IntWritable partNum = new IntWritable();

		key.set("bucket-1");
		partNum.set(0);
		writer.append(key, partNum);

		key.set("bucket-2");
		partNum.set(0);
		writer.append(key, partNum);

		key.set("bucket-3");
		partNum.set(1);
		writer.append(key, partNum);

		key.set("bucket-4");
		partNum.set(2);
		writer.append(key, partNum);

		key.set("bucket-5");
		partNum.set(2);
		writer.append(key, partNum);

		key.set("bucket-6");
		partNum.set(2);
		writer.append(key, partNum);

		writer.close();

		job.setNumReduceTasks(40);


		partitioner.configure(job);


		Text fileName = new Text();

		key.set("bucket-1");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(0));
		}


		key.set("bucket-2");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(0));
		}


		key.set("bucket-3");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(1));
		}


		key.set("bucket-4");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(2));
		}


		key.set("bucket-5");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(2));
		}


		key.set("bucket-6");

		for (int file = 0; file < 4; file++) {
			fileName.set("file" + file);
			assertThat(partitioner.getPartition(key, fileName, 3), equalTo(2));
		}
	}

	@Test
	public void noDupes() throws IOException {

		Writer writer = SequenceFile.createWriter(fs, job, partitionMap, Text.class, IntWritable.class);

		Text key = new Text();
		IntWritable value = new IntWritable();

		key.set("bucket-1");
		value.set(0);
		writer.append(key, value);

		key.set("bucket-2");
		value.set(0);
		writer.append(key, value);

		key.set("bucket-2");
		value.set(1);
		writer.append(key, value);

		writer.close();

		job.setNumReduceTasks(3);

		try {
			partitioner.configure(job);
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("bucket-2")) {
				throw e;
			}
		}
	}

	@Test
	public void partitionTooLow() throws IOException {

		Writer writer = SequenceFile.createWriter(fs, job, partitionMap, Text.class, IntWritable.class);

		Text key = new Text();
		IntWritable partNum = new IntWritable();

		key.set("bucket-1");
		partNum.set(0);
		writer.append(key, partNum);

		key.set("bucket-2");
		partNum.set(0);
		writer.append(key, partNum);

		key.set("bucket-4");
		partNum.set(2);
		writer.append(key, partNum);

		key.set("bucket-5");
		partNum.set(2);
		writer.append(key, partNum);

		key.set("bucket-6");
		partNum.set(-1);
		writer.append(key, partNum);

		writer.close();


		job.setNumReduceTasks(3);

		try {
			partitioner.configure(job);
			fail("No such thing as a negitave partition");
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("Partition -1")) {
				throw e;
			}
		}
	}

	@Test
	public void partitionTooHigh() throws IOException {

		Writer writer = SequenceFile.createWriter(fs, job, partitionMap, Text.class, IntWritable.class);

		Text key = new Text();
		IntWritable partNum = new IntWritable();

		key.set("bucket-1");
		partNum.set(0);
		writer.append(key, partNum);

		key.set("bucket-2");
		partNum.set(0);
		writer.append(key, partNum);

		key.set("bucket-4");
		partNum.set(2);
		writer.append(key, partNum);

		key.set("bucket-5");
		partNum.set(2);
		writer.append(key, partNum);

		key.set("bucket-6");
		partNum.set(3);
		writer.append(key, partNum);

		writer.close();


		job.setNumReduceTasks(3);

		try {
			partitioner.configure(job);
			fail("Parition with id 3 is not allowed with 3 reduce tasks");
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("Partition 3")) {
				throw e;
			}
		}
	}
}

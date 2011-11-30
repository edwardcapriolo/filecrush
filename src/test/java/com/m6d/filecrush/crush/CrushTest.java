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
import static java.lang.System.currentTimeMillis;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.m6d.filecrush.crush.Crush;
import com.m6d.filecrush.crush.MapperCounter;

@SuppressWarnings("deprecation")
public class CrushTest {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private JobConf job;

	private FileSystem fileSystem;

	private String javaIoTmpDir;

	@Before
	public void setupJob() throws IOException {
		job = new JobConf(false);

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.setInt("mapred.reduce.tasks", 5);
		job.setLong("dfs.block.size", 50);

		FileSystem delegate = FileSystem.get(job);

		fileSystem = new SortingFileSystem(delegate);

		/*
		 * Set the working directory so that all relative paths are rooted in the tmp dir. This will keep the file system clean of
		 * temporary test files.
		 */
		FileSystem.get(job).setWorkingDirectory(new Path(tmp.getRoot().getAbsolutePath()));
	}

	@Before
	public void setJavaIoTmpDir() {
		javaIoTmpDir = System.setProperty("java.io.tmpdir", tmp.getRoot().getAbsolutePath());
	}

	@After
	public void restoreJavaIoTmpDir() {
		System.setProperty("java.io.tmpdir", javaIoTmpDir);
	}

	private void run(String... args) throws Exception {
		ToolRunner.run(job, new Crush(), args);
	}

	@Test
	public void backwardsCompatibleInvocationBadSrcDir() throws Exception {
		try {
			run("does-not-exist", tmp.getRoot().getAbsolutePath(), "80");
			fail();
		} catch (IOException e) {
			if (!e.getMessage().contains("does-not-exist")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationBadNumberOfTasks() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "not a number");
			fail();
		} catch (NumberFormatException e) {
			if (!e.getMessage().contains("not a number")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationNegativeTasks() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "-1");
			fail();
		} catch (UnrecognizedOptionException e) {
			if (!e.getMessage().contains("-1")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationZeroTasks() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "0");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("0")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationHugeTasks() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "4001");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("4001")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationBadSrcDirWithType() throws Exception {
		try {
			run("does-not-exist", tmp.getRoot().getAbsolutePath(), "80", "TEXT");
			fail();
		} catch (IOException e) {
			if (!e.getMessage().contains("does-not-exist")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationBadNumberOfTasksWithType() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "not a number", "TEXT");
			fail();
		} catch (NumberFormatException e) {
			if (!e.getMessage().contains("not a number")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationNegativeTasksWithType() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "-1", "TEXT");
			fail();
		} catch (UnrecognizedOptionException e) {
			if (!e.getMessage().contains("-1")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationZeroTasksWithType() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "0", "TEXT");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("0")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationHugeHugeTasksWithType() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "4001", "TEXT");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("4001")) {
				throw e;
			}
		}
	}

	@Test
	public void backwardsCompatibleInvocationBadType() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "80", "NEITHER_TEXT_OR_SEQUENCE");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("NEITHER_TEXT_OR_SEQUENCE")) {
				throw e;
			}
		}
	}

	@Test
	public void invocationBadSrcDir() throws Exception {
		try {
			run("--threshold=0.9", "does-not-exist", tmp.getRoot().getAbsolutePath(), "20101116123015");
			fail();
		} catch (IOException e) {
			if (!e.getMessage().contains("does-not-exist")) {
				throw e;
			}
		}
	}

	@Test
	public void invocationBadTimestamp() throws Exception {
		try {
			run("--threshold=0.9", tmp.newFolder("in").getAbsolutePath(), new File(tmp.getRoot(), "out").getAbsolutePath(), "not a number");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("not a number")) {
				throw e;
			}
		}
	}

	@Test
	public void invocationShortTimestamp() throws Exception {
		try {
			run(tmp.newFolder("in").getAbsolutePath(), new File(tmp.getRoot(), "out").getAbsolutePath(), "2010111612301");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("2010111612301")) {
				throw e;
			}
		}
	}

	@Test
	public void invocationLongTimestamp() throws Exception {
		try {
			run("--threshold=0.5", tmp.newFolder("in").getAbsolutePath(), new File(tmp.getRoot(), "out").getAbsolutePath(), "201011161230150");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("201011161230150")) {
				throw e;
			}
		}
	}

	@Test
	public void dirWithNoMatchingRegex() throws Exception {
		/*
		 * Create a non-empty directory.
		 */
		File src = tmp.newFolder("src");
		tmp.newFolder("src/foo");
		tmp.newFile("src/foo/file");

		try {
			run("--regex", ".+/in",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--threshold", "0.5",
					"--max-file-blocks", "100",
					src.getAbsolutePath(), "out", "20101116123015");

			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("src/foo")) {
				throw e;
			}
		}
	}

	@Test
	public void bucketing() throws Exception {
		File in = tmp.newFolder("in");

		Counters expectedCounters = new Counters();
		List<String> expectedBucketFiles = new ArrayList<String>();

		/*
		 * Create a hierarchy of directories. Directories are distinguished by a trailing slash in these comments.
		 *
		 *	1/
		 *			1.1/
		 *					file1 10 bytes
		 *					file2 20 bytes
		 *					file3 30 bytes
		 *					file4 41 bytes
		 *					file5 15 bytes
		 *					file6 30 bytes
		 *					file7	20 bytes
		 *			1.2/
		 *					file1 20 bytes
		 *					file2 10 bytes
		 *			1.3/
		 *	2/
		 *			file1 70 bytes
		 *			file2 30 bytes
		 *			file3 25 bytes
		 *			file4 30 bytes
		 *			file5 35 bytes
		 *			2.1/
		 *					file1 10 bytes
		 *			2.2/
		 *					file1 25 bytes
		 *					file2 15 bytes
		 *					file3 35 bytes
		 *			2.3/
		 *					file1 41 bytes
		 *					file2 10 bytes
		 *			2.4/
		 *					2.4.1/
		 *							file1 100 bytes
		 *							file2	30 bytes
		 *					2.4.2/
		 *							file1 20 bytes
		 *							file2 20 bytes
		 *							file3 10 bytes
		 */

		/*
		 * in contains 2 dirs and no files so it is skipped.
		 *
		 * 	in/
		 * 			1/
		 * 			2/
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);

		tmp.newFolder("in/1");
		File dir2 = tmp.newFolder("in/2");


		/*
		 * in/1 contains three dirs and no files so it is skipped.
		 *
		 * 	in/
		 * 			1/
		 * 					1.1/
		 * 					1.2/
		 * 					1.3/
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);

		File dir1_1 = tmp.newFolder("in/1/1.1");
		File dir1_2 = tmp.newFolder("in/1/1.2");
		tmp.newFolder("in/1/1.3");


		/*
		 * in/2 contains five files and four dirs.
		 *
		 * 	in/
		 * 			2/
		 *					file1 70 bytes
		 *					file2 30 bytes
		 *					file3 25 bytes
		 *					file4 30 bytes
		 *					file5 35 bytes
		 * 					2.1/
		 * 					2.2/
		 * 					2.3/
		 * 					2.4/
		 *
		 * 	0						1						2
		 * 	file5	35		file2 30		file4 30
		 * 							file3 25
		 *
		 * Buckets 0 and 2 have a single file each so they are ignored.
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_ELIGIBLE, 1);

		expectedCounters.incrCounter(MapperCounter.FILES_FOUND, 5);
		expectedCounters.incrCounter(MapperCounter.FILES_ELIGIBLE, 2);
		expectedCounters.incrCounter(MapperCounter.FILES_SKIPPED, 3);

		File dir2_1 = tmp.newFolder("in/2/2.1");
		File dir2_2 = tmp.newFolder("in/2/2.2");
		File dir2_3 = tmp.newFolder("in/2/2.3");
		tmp.newFolder("in/2/2.4");

		createFile(dir2, "file1", 70);
		createFile(dir2, "file2", 30);
		createFile(dir2, "file3", 25);
		createFile(dir2, "file4", 30);
		createFile(dir2, "file5", 35);

		expectedBucketFiles.add(format("%s	%s", dir2.getAbsolutePath() + "-1", new File(dir2, "file2").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir2.getAbsolutePath() + "-1", new File(dir2, "file3").getAbsolutePath()));


		/*
		 * in/1/1.1 contains seven files and no dirs.
		 *
		 * 	in/
		 * 			1/
		 * 					1.1/
		 *							file1 10 bytes
		 *							file2 20 bytes
		 *							file3 30 bytes
		 *							file4 41 bytes
		 *							file5 15 bytes
		 *							file6 30 bytes
		 *							file7	20 bytes
		 *
		 * 	0						1						2
		 * 	file3 30		file6 30		file2 20
		 * 	file5 15		file1 10		file7 20
		 *
		 * file4 is > 50 * 0.8 so it is ignored.
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_ELIGIBLE, 1);

		expectedCounters.incrCounter(MapperCounter.FILES_FOUND, 7);
		expectedCounters.incrCounter(MapperCounter.FILES_ELIGIBLE, 6);
		expectedCounters.incrCounter(MapperCounter.FILES_SKIPPED, 1);

		createFile(dir1_1, "file1", 10);
		createFile(dir1_1, "file2", 20);
		createFile(dir1_1, "file3", 30);
		createFile(dir1_1, "file4", 41);
		createFile(dir1_1, "file5", 15);
		createFile(dir1_1, "file6", 30);
		createFile(dir1_1, "file7", 20);

		expectedBucketFiles.add(format("%s	%s", dir1_1.getAbsolutePath() + "-0", new File(dir1_1, "file3").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir1_1.getAbsolutePath() + "-0", new File(dir1_1, "file5").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir1_1.getAbsolutePath() + "-1", new File(dir1_1, "file6").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir1_1.getAbsolutePath() + "-1", new File(dir1_1, "file1").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir1_1.getAbsolutePath() + "-2", new File(dir1_1, "file2").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir1_1.getAbsolutePath() + "-2", new File(dir1_1, "file7").getAbsolutePath()));


		/*
		 * in/1/1.2 contains to files.
		 *
		 * 	in/
		 * 			1/
		 * 					1.2/
		 *							file1 20 bytes
		 *							file2 10 bytes
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_ELIGIBLE, 1);

		expectedCounters.incrCounter(MapperCounter.FILES_FOUND, 2);
		expectedCounters.incrCounter(MapperCounter.FILES_ELIGIBLE, 2);

		createFile(dir1_2, "file1", 20);
		createFile(dir1_2, "file2", 10);

		expectedBucketFiles.add(format("%s	%s", dir1_2.getAbsolutePath() + "-0", new File(dir1_2, "file1").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir1_2.getAbsolutePath() + "-0", new File(dir1_2, "file2").getAbsolutePath()));


		/*
		 * in/1/1.3 is empty.
		 *
		 * 	in/
		 * 			1/
		 * 					1.3/
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);

		tmp.newFolder("in/1/1.3");


		/*
		 * in/2/2.1 contains on file.
		 *
		 * 	in/
		 * 			2/
		 * 					2.1/
		 *							file1 10 bytes
		 *
		 * Single file dirs are ignored.
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);

		expectedCounters.incrCounter(MapperCounter.FILES_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.FILES_SKIPPED, 1);

		createFile(dir2_1, "file1", 10);


		/*
		 * in/2/2.2 contains three files.
		 *
		 * 	in/
		 * 			2/
		 * 					2.2/
		 *							file1 25 bytes
		 *							file2 15 bytes
		 *							file3 35 bytes
		 *
		 * 	0						1
		 * 	file3 35		file1 25
		 * 							file2 15
		 *
		 * Bucket 0 with a single file is ignored.
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_ELIGIBLE, 1);

		expectedCounters.incrCounter(MapperCounter.FILES_FOUND, 3);
		expectedCounters.incrCounter(MapperCounter.FILES_ELIGIBLE, 2);
		expectedCounters.incrCounter(MapperCounter.FILES_SKIPPED, 1);

		createFile(dir2_2, "file1", 25);
		createFile(dir2_2, "file2", 15);
		createFile(dir2_2, "file3", 35);

		expectedBucketFiles.add(format("%s	%s", dir2_2.getAbsolutePath() + "-1", new File(dir2_2, "file1").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir2_2.getAbsolutePath() + "-1", new File(dir2_2, "file2").getAbsolutePath()));


		/*
		 * in/2/2.3 contains 2 files.
		 *
		 * 	in/
		 * 			2/
		 * 					2.3/
		 *							file1 41 bytes
		 *							file2 10 bytes
		 *
		 * file1 is too big and leaving file2 as a single file, which is also ignored.
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);

		expectedCounters.incrCounter(MapperCounter.FILES_FOUND, 2);
		expectedCounters.incrCounter(MapperCounter.FILES_SKIPPED, 2);

		createFile(dir2_3, "file1", 41);
		createFile(dir2_3, "file2", 10);


		/*
		 * in/2/2.4 contains two sub directories and no files.
		 *
		 * 	in/
		 * 			2/
		 *					2.4/
		 *							2.4.1/
		 *							2.4.2/
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);

		tmp.newFolder("in/2/2.4");

		File dir2_4_1 = tmp.newFolder("in/2/2.4/2.4.1");
		File dir2_4_2 = tmp.newFolder("in/2/2.4/2.4.2");


		/*
		 * 	in/
		 * 			2/
		 *					2.4/
		 *							2.4.1/
		 *									file1 100 bytes
		 *									file2	30 bytes
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);

		expectedCounters.incrCounter(MapperCounter.FILES_FOUND, 2);
		expectedCounters.incrCounter(MapperCounter.FILES_SKIPPED, 2);

		createFile(dir2_4_1, "file1", 100);
		createFile(dir2_4_1, "file2", 30);


		/*
		 * 	in/
		 * 			2/
		 *					2.4/
		 *							2.4.2/
		 *									file1 20 bytes
		 *									file2 20 bytes
		 *									file3 10 bytes
		 *	0
		 *	file1 20
		 *	file2 20
		 *	file3 10
		 */
		expectedCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		expectedCounters.incrCounter(MapperCounter.DIRS_ELIGIBLE, 1);

		expectedCounters.incrCounter(MapperCounter.FILES_FOUND, 3);
		expectedCounters.incrCounter(MapperCounter.FILES_ELIGIBLE, 3);

		createFile(dir2_4_2, "file1", 20);
		createFile(dir2_4_2, "file2", 20);
		createFile(dir2_4_2, "file3", 10);

		expectedBucketFiles.add(format("%s	%s", dir2_4_2.getAbsolutePath() + "-0", new File(dir2_4_2, "file1").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir2_4_2.getAbsolutePath() + "-0", new File(dir2_4_2, "file2").getAbsolutePath()));
		expectedBucketFiles.add(format("%s	%s", dir2_4_2.getAbsolutePath() + "-0", new File(dir2_4_2, "file3").getAbsolutePath()));


		Crush crush = new Crush();

		crush.setConf(job);
		crush.setFileSystem(fileSystem);

		/*
		 * Call these in the same order that run() does.
		 */
		crush.createJobConfAndParseArgs("--compress=none", "--max-file-blocks=1", in.getAbsolutePath(), new File(tmp.getRoot(), "out").getAbsolutePath(), "20101124171730");
		crush.writeDirs();


		/*
		 * Verify bucket contents.
		 */

		List<String> actualBucketFiles = new ArrayList<String>();

		Text key = new Text();
		Text value = new Text();

		Reader reader = new Reader(FileSystem.get(job), crush.getBucketFiles(), job);

		while(reader.next(key, value)) {
			actualBucketFiles.add(format("%s\t%s", key, value));
		}

		reader.close();

		Collections.sort(expectedBucketFiles);
		Collections.sort(actualBucketFiles);

		assertThat(actualBucketFiles, equalTo(expectedBucketFiles));

		/*
		 * Verify the partition map.
		 */
		Reader partitionMapReader = new Reader(FileSystem.get(job), crush.getPartitionMap(), job);

		IntWritable partNum = new IntWritable();

		Map<String, Integer> actualPartitions = new HashMap<String, Integer>();

		while (partitionMapReader.next(key, partNum)) {
			actualPartitions.put(key.toString(), partNum.get());
		}

		partitionMapReader.close();

		/*
		 * These crush files need to allocated into 5 partitions:
		 *
		 * in/2-1						55 bytes
		 * in/1/1.1-0				45 bytes
		 * in/1/1.1-2				40 bytes
		 * in/1/1.1-1				40 bytes
		 * in/1/1.2-0				30 bytes
		 * in/2/2.2-1				40 bytes
		 * in/2/2.4/2.4.2-0	50 bytes
		 *
		 * 	0							1											2								3								4
		 * 	in/2-1 55			in/2/2.4/2.4.2-0	50	in/1/1.1-0	45	in/1/1.1-2	40	in/1/1.1-1	40
		 * 																											in/2/2.2-1	40	in/1/1.2-0	39
		 */
		Map<String, Integer> expectedPartitions = new HashMap<String, Integer>();

		expectedPartitions.put(dir2.getAbsolutePath() + "-1",			0);
		expectedPartitions.put(dir2_4_2.getAbsolutePath() + "-0",	1);
		expectedPartitions.put(dir1_1.getAbsolutePath() + "-0",		2);
		expectedPartitions.put(dir1_1.getAbsolutePath() + "-2",		3);
		expectedPartitions.put(dir2_2.getAbsolutePath() + "-1",		3);
		expectedPartitions.put(dir1_1.getAbsolutePath() + "-1",		4);
		expectedPartitions.put(dir1_2.getAbsolutePath() + "-0",		4);

		assertThat(actualPartitions, equalTo(expectedPartitions));


		/*
		 * Verify counters.
		 */
		Counters actualCounters = new Counters();

		DataInputStream countersStream = FileSystem.get(job).open(crush.getCounters());

		actualCounters.readFields(countersStream);

		countersStream.close();

		assertThat(actualCounters, equalTo(expectedCounters));
	}

	/**
	 * Returns a qualified file status, just like {@link FileSystem#listStatus(Path)} does.
	 */
	private static FileStatus createFile(File dir, String fileName, int size) {
		File file = new File(dir, fileName);

		try {
			FileOutputStream os = new FileOutputStream(file);

			os.write(new byte[size]);

			os.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return new FileStatus(size, false, 3, 1024 * 1024 * 60, currentTimeMillis(), new Path("hdfs://hostname.pvt:12345" + file.getAbsolutePath()));
	}

	/**
	 * This exists only so we can impose a specific order on the files that are listed.
	 */
	private static class SortingFileSystem extends FileSystem {

		private final FileSystem delegate;

		public SortingFileSystem(FileSystem delegate) {
			super();

			this.delegate = delegate;
		}

		@Override
		public FileStatus[] listStatus(Path arg0) throws IOException {
			FileStatus[] contents = delegate.listStatus(arg0);

			Arrays.sort(contents);

			return contents;
		}

		@Override
		public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
			return delegate.append(arg0, arg1, arg2);
		}

		@Override
		public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
			return delegate.append(f, bufferSize);
		}

		@Override
		public FSDataOutputStream append(Path f) throws IOException {
			return delegate.append(f);
		}

		@Override
		public void close() throws IOException {
			delegate.close();
		}

		@Override
		public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
			delegate.completeLocalOutput(fsOutputFile, tmpLocalFile);
		}

		@Override
		public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
			delegate.copyFromLocalFile(delSrc, overwrite, src, dst);
		}

		@Override
		public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
			delegate.copyFromLocalFile(delSrc, overwrite, srcs, dst);
		}

		@Override
		public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
			delegate.copyFromLocalFile(delSrc, src, dst);
		}

		@Override
		public void copyFromLocalFile(Path src, Path dst) throws IOException {
			delegate.copyFromLocalFile(src, dst);
		}

		@Override
		public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
			delegate.copyToLocalFile(delSrc, src, dst);
		}

		@Override
		public void copyToLocalFile(Path src, Path dst) throws IOException {
			delegate.copyToLocalFile(src, dst);
		}

		@Override
		public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress) throws IOException {
			return delegate.create(f, overwrite, bufferSize, progress);
		}

		@Override
		public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
				Progressable progress) throws IOException {
			return delegate.create(f, overwrite, bufferSize, replication, blockSize, progress);
		}

		@Override
		public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
				throws IOException {
			return delegate.create(f, overwrite, bufferSize, replication, blockSize);
		}

		@Override
		public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
			return delegate.create(f, overwrite, bufferSize);
		}

		@Override
		public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
			return delegate.create(f, overwrite);
		}

		@Override
		public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3, short arg4, long arg5,
				Progressable arg6) throws IOException {
			return delegate.create(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
		}

		@Override
		public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
			return delegate.create(f, progress);
		}

		@Override
		public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
			return delegate.create(f, replication, progress);
		}

		@Override
		public FSDataOutputStream create(Path f, short replication) throws IOException {
			return delegate.create(f, replication);
		}

		@Override
		public FSDataOutputStream create(Path f) throws IOException {
			return delegate.create(f);
		}

		@Override
		public boolean createNewFile(Path f) throws IOException {
			return delegate.createNewFile(f);
		}

		@Override
		public boolean delete(Path arg0, boolean arg1) throws IOException {
			return delegate.delete(arg0, arg1);
		}

		@Override
		public boolean delete(Path arg0) throws IOException {
			return delegate.delete(arg0);
		}

		@Override
		public boolean deleteOnExit(Path f) throws IOException {
			return delegate.deleteOnExit(f);
		}

		@Override
		public boolean equals(Object obj) {
			return delegate.equals(obj);
		}

		@Override
		public boolean exists(Path arg0) throws IOException {
			return delegate.exists(arg0);
		}

		@Override
		public long getBlockSize(Path f) throws IOException {
			return delegate.getBlockSize(f);
		}

		@Override
		public Configuration getConf() {
			return delegate.getConf();
		}

		@Override
		public ContentSummary getContentSummary(Path arg0) throws IOException {
			return delegate.getContentSummary(arg0);
		}

		@Override
		public long getDefaultBlockSize() {
			return delegate.getDefaultBlockSize();
		}

		@Override
		public short getDefaultReplication() {
			return delegate.getDefaultReplication();
		}

		@Override
		public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
			return delegate.getFileBlockLocations(file, start, len);
		}

		@Override
		public FileChecksum getFileChecksum(Path f) throws IOException {
			return delegate.getFileChecksum(f);
		}

		@Override
		public FileStatus getFileStatus(Path arg0) throws IOException {
			return delegate.getFileStatus(arg0);
		}

		@Override
		public Path getHomeDirectory() {
			return delegate.getHomeDirectory();
		}

		@Override
		public long getLength(Path f) throws IOException {
			return delegate.getLength(f);
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		public short getReplication(Path src) throws IOException {
			return delegate.getReplication(src);
		}

		@Override
		public URI getUri() {
			return delegate.getUri();
		}

		@Override
		public long getUsed() throws IOException {
			return delegate.getUsed();
		}

		@Override
		public Path getWorkingDirectory() {
			return delegate.getWorkingDirectory();
		}

		@Override
		public FileStatus[] globStatus(Path arg0, PathFilter arg1) throws IOException {
			return delegate.globStatus(arg0, arg1);
		}

		@Override
		public FileStatus[] globStatus(Path pathPattern) throws IOException {
			return delegate.globStatus(pathPattern);
		}

		@Override
		public int hashCode() {
			return delegate.hashCode();
		}

		@Override
		public void initialize(URI name, Configuration conf) throws IOException {
			delegate.initialize(name, conf);
		}

		@Override
		public boolean isDirectory(Path arg0) throws IOException {
			return delegate.isDirectory(arg0);
		}

		@Override
		public boolean isFile(Path arg0) throws IOException {
			return delegate.isFile(arg0);
		}

		@Override
		public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
			return delegate.listStatus(f, filter);
		}

		@Override
		public FileStatus[] listStatus(Path[] arg0, PathFilter arg1) throws IOException {
			return delegate.listStatus(arg0, arg1);
		}

		@Override
		public FileStatus[] listStatus(Path[] files) throws IOException {
			return delegate.listStatus(files);
		}

		@Override
		public Path makeQualified(Path path) {
			return delegate.makeQualified(path);
		}

		@Override
		public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
			return delegate.mkdirs(arg0, arg1);
		}

		@Override
		public boolean mkdirs(Path f) throws IOException {
			return delegate.mkdirs(f);
		}

		@Override
		public void moveFromLocalFile(Path src, Path dst) throws IOException {
			delegate.moveFromLocalFile(src, dst);
		}

		@Override
		public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
			delegate.moveFromLocalFile(srcs, dst);
		}

		@Override
		public void moveToLocalFile(Path src, Path dst) throws IOException {
			delegate.moveToLocalFile(src, dst);
		}

		@Override
		public FSDataInputStream open(Path arg0, int arg1) throws IOException {
			return delegate.open(arg0, arg1);
		}

		@Override
		public FSDataInputStream open(Path f) throws IOException {
			return delegate.open(f);
		}

		@Override
		public boolean rename(Path arg0, Path arg1) throws IOException {
			return delegate.rename(arg0, arg1);
		}

		@Override
		public void setConf(Configuration conf) {
			if (null != delegate) {
				delegate.setConf(conf);
			}
		}

		@Override
		public void setOwner(Path p, String username, String groupname) throws IOException {
			delegate.setOwner(p, username, groupname);
		}

		@Override
		public void setPermission(Path p, FsPermission permission) throws IOException {
			delegate.setPermission(p, permission);
		}

		@Override
		public boolean setReplication(Path src, short replication) throws IOException {
			return delegate.setReplication(src, replication);
		}

		@Override
		public void setTimes(Path p, long mtime, long atime) throws IOException {
			delegate.setTimes(p, mtime, atime);
		}

		@Override
		public void setVerifyChecksum(boolean verifyChecksum) {
			delegate.setVerifyChecksum(verifyChecksum);
		}

		@Override
		public void setWorkingDirectory(Path arg0) {
			delegate.setWorkingDirectory(arg0);
		}

		@Override
		public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
			return delegate.startLocalOutput(fsOutputFile, tmpLocalFile);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}


	}
}

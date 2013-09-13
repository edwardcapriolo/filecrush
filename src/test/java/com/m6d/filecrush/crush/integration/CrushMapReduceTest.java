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

package com.m6d.filecrush.crush.integration;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.m6d.filecrush.crush.Crush;
import com.m6d.filecrush.crush.MapperCounter;
import com.m6d.filecrush.crush.ReducerCounter;
import com.m6d.filecrush.crush.CrushReducerParameterizedTest.CustomCompressionCodec;
import com.m6d.filecrush.crush.CrushReducerParameterizedTest.CustomWritable;


/**
 * Mention the junit runner explicitly since this test inherits from the junit 3 base class and since the junit 3 runner does not
 * have all the extra goodies, like support for {@link Rule}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("deprecation")
public class CrushMapReduceTest extends HadoopTestCase {

	private static final StaticTemporaryFolder TMP = new StaticTemporaryFolder();

	private static String ORIG_TMP;

	private JobConf job;

	/**
	 * Absolute path to the home dir. No scheme or authority.
	 */
	private String homeDir;

	private DefaultCodec defaultCodec;

	private CustomCompressionCodec customCodec;

	public CrushMapReduceTest() throws IOException {
		super(CLUSTER_MR, DFS_FS, /* task trackers */ 4, /* data nodes */ 4);
	}

	@BeforeClass
	public static void setTmpDir() {
		try {
			TMP.before();
		} catch (Throwable e) {
			throw new RuntimeException("Could not setup tmp dir", e);
		}

		ORIG_TMP = System.setProperty("java.io.tmpdir", TMP.getRoot().getAbsolutePath());

		File logsDir = TMP.newFolder("logs");

		System.setProperty("hadoop.log.dir", logsDir.getAbsolutePath());

		File dfsDir = TMP.newFolder("dfs");

		System.setProperty("test.build.data", dfsDir.getAbsolutePath());
	}

	@AfterClass
	public static void restoreIoTmp() {
		try {
			TMP.after();
		} catch (Throwable e) {
			throw new RuntimeException("Could not clean up tmp dir", e);
		} finally {
			System.setProperty("java.io.tmpdir", ORIG_TMP);
		}
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();

		job = createJobConf();

		job.setBoolean("mapred.output.compress", true);
		job.set("mapred.output.compression.type", CompressionType.BLOCK.name());
		job.set("mapred.output.compression.codec", CustomCompressionCodec.class.getName());

		FileSystem fs = getFileSystem();

		Path homeDirPath = fs.makeQualified(new Path("."));

		homeDir = homeDirPath.toUri().getPath();

		fs.delete(homeDirPath,  true);

		defaultCodec = new DefaultCodec();
		defaultCodec.setConf(job);

		customCodec = new CustomCompressionCodec();
		customCodec.setConf(job);
	}

	@Override
	@After
	public void tearDown() throws Exception {
		/*
		 * Make sure we have no scratch data left over.
		 */
		FileStatus[] tmpFiles = getFileSystem().listStatus(new Path("tmp"));

		if (tmpFiles != null) {
			assertThat(Arrays.toString(FileUtil.stat2Paths(tmpFiles)), tmpFiles.length, equalTo(0));
		}

		super.tearDown();

		/*
		 * Let the various services shut down so TemporaryFolder can delete things.
		 */
		Thread.sleep(2000);
	}

	/**
	 * Writes test files in a subdir of {@link #TMP} called "in".
	 */
	private void writeFiles(boolean sequence, boolean text, boolean huge) throws IOException {
		/*
		 * We setup a few directories to exercise regexes. In this comment, dirs are distinguished by a trailing slash. The
		 * file name is followed by the bucket id.
		 *
		 * 	file70
		 * 	file71
		 * 	file72
		 * 	huge
		 * 	dir/
		 * 			file10
		 * 			file11
		 * 			file12
		 * 			file13
		 * 			subdir/
		 * 					file20
		 * 					file21
		 * 					file22
		 * 					file23
		 * 					file24
		 * 					subsubdir/
		 * 							file30
		 * 							file31
		 * 							file32
		 * 							file33
		 * 							file34
		 * 					other/
		 * 							file40
		 * 							file41
		 * 							file42
		 * 							file43
		 * 			other/
		 * 					file50
		 * 					file51
		 * 					file52
		 * 					file53
		 * 					file54
		 * 					file55
		 *			skipped/
		 *					file80
		 * 	text/
		 * 			file60
		 * 			file61
		 * 			file62
		 * 			file63
		 */

		/*
		 * Files in the top level dir.
		 */
		if (sequence) {
			writeFile("in/file70", Format.SEQUENCE);
			writeFile("in/file71", Format.SEQUENCE);
			writeFile("in/file72", Format.SEQUENCE);
		}

		if (huge) {
			writeHugeFile("in/huge", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		}

		/*
		 * 	dir/
		 * 			file10
		 * 			file11
		 * 			file12
		 * 			file13
		 *
		 * These files match the first regex.
		 */
		if (text) {
			writeFile("in/dir/file10", Format.TEXT);
			writeFile("in/dir/file11", Format.TEXT);
			writeFile("in/dir/file12", Format.TEXT);
			writeFile("in/dir/file13", Format.TEXT);
		}


		/*
		 * dir/
		 * 			subdir/
		 * 					file20
		 * 					file21
		 * 					file22
		 * 					file23
		 * 					file24
		 */
		if (sequence) {
			writeFile("in/dir/subdir/file20", Format.SEQUENCE);
			writeFile("in/dir/subdir/file21", Format.SEQUENCE);
			writeFile("in/dir/subdir/file22", Format.SEQUENCE);
			writeFile("in/dir/subdir/file23", Format.SEQUENCE);
			writeFile("in/dir/subdir/file24", Format.SEQUENCE);
		}

		/*
		 * dir/
		 * 			subdir/
		 * 					subsubdir/
		 * 							file30
		 * 							file31
		 * 							file32
		 * 							file33
		 * 							file34
		 */
		if (sequence) {
			writeFile("in/dir/subdir/subsubdir/file30", Format.SEQUENCE);
			writeFile("in/dir/subdir/subsubdir/file31", Format.SEQUENCE);
			writeFile("in/dir/subdir/subsubdir/file32", Format.SEQUENCE);
			writeFile("in/dir/subdir/subsubdir/file33", Format.SEQUENCE);
			writeFile("in/dir/subdir/subsubdir/file34", Format.SEQUENCE);
		}

		/*
		 * dir/
		 * 			subdir/
		 * 					other/
		 * 							file40
		 * 							file41
		 * 							file42
		 * 							file43
		 */
		if (sequence) {
			writeFile("in/dir/subdir/other/file40", Format.SEQUENCE);
			writeFile("in/dir/subdir/other/file41", Format.SEQUENCE);
			writeFile("in/dir/subdir/other/file42", Format.SEQUENCE);
			writeFile("in/dir/subdir/other/file43", Format.SEQUENCE);
		}

		/*
		 * dir/
		 * 			other/
		 * 					file50
		 * 					file51
		 * 					file52
		 * 					file53
		 * 					file54
		 * 					file55
		 */
		if (sequence) {
			writeFile("in/dir/other/file50", Format.SEQUENCE);
			writeFile("in/dir/other/file51", Format.SEQUENCE);
			writeFile("in/dir/other/file52", Format.SEQUENCE);
			writeFile("in/dir/other/file53", Format.SEQUENCE);
			writeFile("in/dir/other/file54", Format.SEQUENCE);
			writeFile("in/dir/other/file55", Format.SEQUENCE);
		}

		/*
		 * 	dir/
		 *			skipped/
		 *					file80
		 */
		if (sequence) {
			writeFile("in/dir/skipped/file80", Format.SEQUENCE);
		}

		/*
		 * 	text/
		 * 			file60
		 * 			file61
		 * 			file62
		 * 			file63
		 */
		if (text) {
			writeFile("in/text/file60", Format.TEXT);
			writeFile("in/text/file61", Format.TEXT);
			writeFile("in/text/file62", Format.TEXT);
			writeFile("in/text/file63", Format.TEXT);
		}
	}

	@Test
	public void execute() throws Exception {
		writeFiles(true, true, true);

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"--threshold=0.015",
			"--max-file-blocks=1",
			"--verbose",

			"--regex=.+/other",
			"--replacement=${crush.timestamp}-${crush.task.num}-middle-${crush.file.num}-tail",
			"--input-format=" + SequenceFileInputFormat.class.getName(),
			"--output-format=" + TextOutputFormat.class.getName(),

			"--regex=.+/dir",
			"--replacement=secondregex-${crush.timestamp}-${crush.task.num}-${crush.file.num}",
			"--input-format=" + TextInputFormat.class.getName(),
			"--output-format=" + SequenceFileOutputFormat.class.getName(),

			"--regex=.+/dir/([^/]+/)*(.+)",
			"--replacement=thirdregex-$2-${crush.timestamp}-${crush.task.num}-${crush.file.num}",
			"--input-format=" + SequenceFileInputFormat.class.getName(),
			"--output-format=" + SequenceFileOutputFormat.class.getName(),

			"--regex=.+/text",
			"--replacement=fourthregex-${crush.task.num}-${crush.timestamp}-${crush.file.num}",
			"--input-format=" + TextInputFormat.class.getName(),
			"--output-format=" + TextOutputFormat.class.getName(),

			/*
			 * This is the default regex and replacement, which we add last so we can exercise the default logic.
			 */
			"--regex=.+",
			"--replacement=crushed_file-${crush.timestamp}-${crush.task.num}-${crush.file.num}",
			"--input-format=" + SequenceFileInputFormat.class.getName(),
			"--output-format=" + TextOutputFormat.class.getName(),

			"--compress=" + CustomCompressionCodec.class.getName(),

			"in", "out", "20101116153015"
		});


		/*
		 * Crushed files.
		 */
		verifyOutput(homeDir + "/out/dir",									"secondregex-20101116153015-*-*",						Format.TEXT,			Format.SEQUENCE,	customCodec, "file10", "file11", "file12", "file13");
		verifyOutput(homeDir + "/out/dir/subdir",						"thirdregex-subdir-20101116153015-*-*",			Format.SEQUENCE,	Format.SEQUENCE,	customCodec, "file20", "file21", "file22", "file23", "file24");
		verifyOutput(homeDir + "/out/dir/subdir/subsubdir",	"thirdregex-subsubdir-20101116153015-*-*",	Format.SEQUENCE,	Format.SEQUENCE,	customCodec, "file30", "file31", "file32", "file33", "file34");
		verifyOutput(homeDir + "/out/dir/subdir/other",			"20101116153015-*-middle-*-tail",						Format.SEQUENCE,	Format.TEXT, 			customCodec, "file40", "file41", "file42", "file43");
		verifyOutput(homeDir + "/out/dir/other",						"20101116153015-*-middle-*-tail",						Format.SEQUENCE,	Format.TEXT,			customCodec, "file50", "file51", "file52", "file53", "file54", "file55");
		verifyOutput(homeDir + "/out/text",									"fourthregex-*-20101116153015-*",						Format.TEXT,			Format.TEXT,			customCodec, "file60", "file61", "file62", "file63");
		verifyOutput(homeDir + "/out",											"crushed_file-20101116153015-*-*",					Format.SEQUENCE,	Format.TEXT,			customCodec, "file70", "file71", "file72");


		/*
		 * Skipped files should have been moved to the output dir.
		 */
		verifyOutput(homeDir + "/out/dir/skipped", "file80", Format.SEQUENCE, Format.SEQUENCE, defaultCodec, "file80");
		verifyHugeFile(homeDir + "/out/huge", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);


		/*
		 * Crush input files should remain in the input dir.
		 */
		for (String file : new String[] { "file10", "file11", "file12", "file13" }) {
			verifyOutput(homeDir + "/in/dir", file, Format.TEXT, Format.TEXT, null, file);
		}

		for (String file : new String[] { "file20", "file21", "file22", "file23", "file24" }) {
			verifyOutput(homeDir + "/in/dir/subdir", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file30", "file31", "file32", "file33", "file34" }) {
			verifyOutput(homeDir + "/in/dir/subdir/subsubdir", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file40", "file41", "file42", "file43" }) {
			verifyOutput(homeDir + "/in/dir/subdir/other", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file50", "file51", "file52", "file53", "file54", "file55" }) {
			verifyOutput(homeDir + "/in/dir/other", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file60", "file61", "file62", "file63" }) {
			verifyOutput(homeDir + "/in/text", file, Format.TEXT, Format.TEXT, null, file);
		}

		for (String file : new String[] { "file70", "file71", "file72" }) {
			verifyOutput(homeDir + "/in", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}


		Counters jobCounters = crush.getJobCounters();

		assertThat(jobCounters.getCounter(MapperCounter.DIRS_FOUND),			equalTo( 8L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_ELIGIBLE),		equalTo( 7L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_SKIPPED),		equalTo( 1L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_FOUND),			equalTo(33L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE),	equalTo(31L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_SKIPPED),		equalTo( 2L));

		assertThat(jobCounters.getCounter(ReducerCounter.FILES_CRUSHED),		equalTo(  31L));
		assertThat(jobCounters.getCounter(ReducerCounter.RECORDS_CRUSHED),	equalTo(1256L));
	}

	@Test
	public void executeHugeFilesOnly() throws Exception {
		writeHugeFile("in/huge", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		writeHugeFile("in/foo/huge1", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		writeHugeFile("in/foo/huge2", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"--threshold=0.015",
			"--max-file-blocks=1",
			"--verbose",
			"--compress=" + CustomCompressionCodec.class.getName(),

			"in", "out", "20101116153015"
		});


		/*
		 * Skipped files should have been moved to the output dir.
		 */
		verifyHugeFile(homeDir + "/out/huge", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		verifyHugeFile(homeDir + "/out/foo/huge1", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		verifyHugeFile(homeDir + "/out/foo/huge2", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);

		Counters jobCounters = crush.getJobCounters();

		assertThat(jobCounters.getCounter(MapperCounter.DIRS_FOUND),			equalTo(2L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_ELIGIBLE),		equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_SKIPPED),		equalTo(2L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_FOUND),			equalTo(3L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE),	equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_SKIPPED),		equalTo(3L));

		assertThat(jobCounters.getCounter(ReducerCounter.FILES_CRUSHED),		equalTo(0L));
		assertThat(jobCounters.getCounter(ReducerCounter.RECORDS_CRUSHED),	equalTo(0L));
	}

	@Test
	public void executeNoFiles() throws Exception {
		FileSystem.get(job).mkdirs(new Path("in/foo"));
		FileSystem.get(job).mkdirs(new Path("in/hello/world"));

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"--threshold=0.015",
			"--max-file-blocks=1",
			"--verbose",
			"--compress=" + CustomCompressionCodec.class.getName(),

			"in", "out", "20101116153015"
		});

		Counters jobCounters = crush.getJobCounters();

		assertThat(jobCounters.getCounter(MapperCounter.DIRS_FOUND),			equalTo(4L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_ELIGIBLE),		equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_SKIPPED),		equalTo(4L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_FOUND),			equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE),	equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_SKIPPED),		equalTo(0L));

		assertThat(jobCounters.getCounter(ReducerCounter.FILES_CRUSHED),		equalTo(0L));
		assertThat(jobCounters.getCounter(ReducerCounter.RECORDS_CRUSHED),	equalTo(0L));
	}

	@Test
	public void executeClone() throws Exception {
		writeFiles(true, true, true);

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"--threshold=0.015",
			"--max-file-blocks=1",
			"--verbose",

			"--regex=.+/other",
			"--replacement=${crush.timestamp}-${crush.task.num}-middle-${crush.file.num}-tail",
			"--input-format=sequence",
			"--output-format=text",

			"--regex=.+/dir",
			"--replacement=secondregex-${crush.timestamp}-${crush.task.num}-${crush.file.num}",
			"--input-format=text",
			"--output-format=" + SequenceFileOutputFormat.class.getName(),

			"--regex=.+/dir/([^/]+/)*(.+)",
			"--replacement=thirdregex-$2-${crush.timestamp}-${crush.task.num}-${crush.file.num}",
			"--input-format=" + SequenceFileInputFormat.class.getName(),
			"--output-format=sequence",

			"--regex=.+/text",
			"--replacement=fourthregex-${crush.task.num}-${crush.timestamp}-${crush.file.num}",
			"--input-format=" + TextInputFormat.class.getName(),
			"--output-format=" + TextOutputFormat.class.getName(),

			/*
			 * This is the default regex and replacement, which we add last so we can exercise the default logic.
			 */
			"--regex=.+",
			"--replacement=crushed_file-${crush.timestamp}-${crush.task.num}-${crush.file.num}",
			"--input-format=" + SequenceFileInputFormat.class.getName(),
			"--output-format=" + TextOutputFormat.class.getName(),

			"--compress=" + CustomCompressionCodec.class.getName(),

			"--clone",

			"in", "out", "20101116153015"
		});


		/*
		 * Crushed files.
		 */
		verifyOutput(homeDir + "/in/dir",										"secondregex-20101116153015-*-*",						Format.TEXT,			Format.SEQUENCE,	customCodec, "file10", "file11", "file12", "file13");
		verifyOutput(homeDir + "/in/dir/subdir",						"thirdregex-subdir-20101116153015-*-*",			Format.SEQUENCE,	Format.SEQUENCE,	customCodec, "file20", "file21", "file22", "file23", "file24");
		verifyOutput(homeDir + "/in/dir/subdir/subsubdir",	"thirdregex-subsubdir-20101116153015-*-*",	Format.SEQUENCE,	Format.SEQUENCE,	customCodec, "file30", "file31", "file32", "file33", "file34");
		verifyOutput(homeDir + "/in/dir/subdir/other",			"20101116153015-*-middle-*-tail",						Format.SEQUENCE,	Format.TEXT, 			customCodec, "file40", "file41", "file42", "file43");
		verifyOutput(homeDir + "/in/dir/other",							"20101116153015-*-middle-*-tail",						Format.SEQUENCE,	Format.TEXT,			customCodec, "file50", "file51", "file52", "file53", "file54", "file55");
		verifyOutput(homeDir + "/in/text",									"fourthregex-*-20101116153015-*",						Format.TEXT,			Format.TEXT,			customCodec, "file60", "file61", "file62", "file63");
		verifyOutput(homeDir + "/in",												"crushed_file-20101116153015-*-*",					Format.SEQUENCE,	Format.TEXT,			customCodec, "file70", "file71", "file72");


		/*
		 * Skipped files should remain in the input dir.
		 */
		verifyOutput(homeDir + "/in/dir/skipped", "file80", Format.SEQUENCE, Format.SEQUENCE, defaultCodec, "file80");
		verifyHugeFile(homeDir + "/in/huge", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);


		/*
		 * Crush input files should be moved to the clone dir.
		 */
		for (String file : new String[] { "file10", "file11", "file12", "file13" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir", file, Format.TEXT, Format.TEXT, null, file);
		}

		for (String file : new String[] { "file20", "file21", "file22", "file23", "file24" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir/subdir", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file30", "file31", "file32", "file33", "file34" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir/subdir/subsubdir", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file40", "file41", "file42", "file43" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir/subdir/other", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file50", "file51", "file52", "file53", "file54", "file55" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir/other", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file60", "file61", "file62", "file63" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/text", file, Format.TEXT, Format.TEXT, null, file);
		}

		for (String file : new String[] { "file70", "file71", "file72" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}


		Counters jobCounters = crush.getJobCounters();

		assertThat(jobCounters.getCounter(MapperCounter.DIRS_FOUND),			equalTo( 8L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_ELIGIBLE),		equalTo( 7L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_SKIPPED),		equalTo( 1L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_FOUND),			equalTo(33L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE),	equalTo(31L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_SKIPPED),		equalTo( 2L));

		assertThat(jobCounters.getCounter(ReducerCounter.FILES_CRUSHED),		equalTo(  31L));
		assertThat(jobCounters.getCounter(ReducerCounter.RECORDS_CRUSHED),	equalTo(1256L));
	}

	@Test
	public void executeCloneHugeFilesOnly() throws Exception {
		writeHugeFile("in/huge", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		writeHugeFile("in/foo/huge1", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		writeHugeFile("in/foo/huge2", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"--threshold=0.015",
			"--max-file-blocks=1",
			"--verbose",
			"--compress=" + CustomCompressionCodec.class.getName(),
			"--clone",

			"in", "out", "20101116153015"
		});


		/*
		 * Skipped files should remain in the input dir.
		 */
		verifyHugeFile(homeDir + "/in/huge", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		verifyHugeFile(homeDir + "/in/foo/huge1", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);
		verifyHugeFile(homeDir + "/in/foo/huge2", (long) (((float) 0.015) * 1024 * 1024 * 64) + 1);

		Counters jobCounters = crush.getJobCounters();

		assertThat(jobCounters.getCounter(MapperCounter.DIRS_FOUND),			equalTo(2L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_ELIGIBLE),		equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_SKIPPED),		equalTo(2L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_FOUND),			equalTo(3L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE),	equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_SKIPPED),		equalTo(3L));

		assertThat(jobCounters.getCounter(ReducerCounter.FILES_CRUSHED),		equalTo(0L));
		assertThat(jobCounters.getCounter(ReducerCounter.RECORDS_CRUSHED),	equalTo(0L));
	}

	@Test
	public void executeCloneNoFiles() throws Exception {
		FileSystem.get(job).mkdirs(new Path("in/foo"));
		FileSystem.get(job).mkdirs(new Path("in/hello/world"));

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"--threshold=0.015",
			"--max-file-blocks=1",
			"--verbose",
			"--compress=" + CustomCompressionCodec.class.getName(),
			"--clone",

			"in", "out", "20101116153015"
		});

		Counters jobCounters = crush.getJobCounters();

		assertThat(jobCounters.getCounter(MapperCounter.DIRS_FOUND),			equalTo(4L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_ELIGIBLE),		equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_SKIPPED),		equalTo(4L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_FOUND),			equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE),	equalTo(0L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_SKIPPED),		equalTo(0L));

		assertThat(jobCounters.getCounter(ReducerCounter.FILES_CRUSHED),		equalTo(0L));
		assertThat(jobCounters.getCounter(ReducerCounter.RECORDS_CRUSHED),	equalTo(0L));
	}

	@Test
	public void executeBackwardsCompatibleText() throws Exception {
		writeFiles(false, true, false);

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"in", "out", "2", "TEXT"
		});


		/*
		 * Crushed files.
		 */
		verifyOutput(homeDir + "/in/dir",  "crushed_file-*-*-*", Format.TEXT, Format.TEXT, defaultCodec, "file10", "file11", "file12", "file13");
		verifyOutput(homeDir + "/in/text", "crushed_file-*-*-*", Format.TEXT, Format.TEXT, defaultCodec, "file60", "file61", "file62", "file63");


		/*
		 * Crush input files should be moved to the clone dir.
		 */
		for (String file : new String[] { "file10", "file11", "file12", "file13" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir", file, Format.TEXT, Format.TEXT, null, file);
		}

		for (String file : new String[] { "file60", "file61", "file62", "file63" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/text", file, Format.TEXT, Format.TEXT, null, file);
		}


		Counters jobCounters = crush.getJobCounters();

		assertThat(jobCounters.getCounter(MapperCounter.DIRS_FOUND),			equalTo(3L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_ELIGIBLE),		equalTo(2L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_SKIPPED),		equalTo(1L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_FOUND),			equalTo(8L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE),	equalTo(8L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_SKIPPED),		equalTo(0L));

		assertThat(jobCounters.getCounter(ReducerCounter.FILES_CRUSHED),		equalTo(  8L));
		assertThat(jobCounters.getCounter(ReducerCounter.RECORDS_CRUSHED),	equalTo(292L));
	}

	@Test
	public void executeBackwardsCompatibleSequence() throws Exception {
		writeFiles(true, false, false);

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"in", "out", "2"
		});


		/*
		 * Crushed files.
		 */
		verifyOutput(homeDir + "/in/dir/subdir",						"crushed_file-*-*-*", Format.SEQUENCE, Format.SEQUENCE, defaultCodec, "file20", "file21", "file22", "file23", "file24");
		verifyOutput(homeDir + "/in/dir/subdir/subsubdir",	"crushed_file-*-*-*", Format.SEQUENCE, Format.SEQUENCE, defaultCodec, "file30", "file31", "file32", "file33", "file34");
		verifyOutput(homeDir + "/in/dir/subdir/other",			"crushed_file-*-*-*", Format.SEQUENCE, Format.SEQUENCE, defaultCodec, "file40", "file41", "file42", "file43");
		verifyOutput(homeDir + "/in/dir/other",							"crushed_file-*-*-*", Format.SEQUENCE, Format.SEQUENCE, defaultCodec, "file50", "file51", "file52", "file53", "file54", "file55");
		verifyOutput(homeDir + "/in",												"crushed_file-*-*-*", Format.SEQUENCE, Format.SEQUENCE, defaultCodec, "file70", "file71", "file72");


		/*
		 * Skipped files should be copied to the output.
		 */
		verifyOutput(homeDir + "/in/dir/skipped", "file80", Format.SEQUENCE, Format.SEQUENCE, defaultCodec, "file80");


		/*
		 * Crush input files should be moved to the clone dir.
		 */
		for (String file : new String[] { "file20", "file21", "file22", "file23", "file24" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir/subdir", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file30", "file31", "file32", "file33", "file34" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir/subdir/subsubdir", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file40", "file41", "file42", "file43" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir/subdir/other", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file50", "file51", "file52", "file53", "file54", "file55" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in/dir/other", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		for (String file : new String[] { "file70", "file71", "file72" }) {
			verifyOutput(homeDir + "/out" + homeDir + "/in", file, Format.SEQUENCE, Format.SEQUENCE, defaultCodec, file);
		}

		Counters jobCounters = crush.getJobCounters();

		assertThat(jobCounters.getCounter(MapperCounter.DIRS_FOUND),			equalTo( 7L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_ELIGIBLE),		equalTo( 5L));
		assertThat(jobCounters.getCounter(MapperCounter.DIRS_SKIPPED),		equalTo( 2L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_FOUND),			equalTo(24L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE),	equalTo(23L));
		assertThat(jobCounters.getCounter(MapperCounter.FILES_SKIPPED),		equalTo( 1L));

		assertThat(jobCounters.getCounter(ReducerCounter.FILES_CRUSHED),		equalTo( 23L));
		assertThat(jobCounters.getCounter(ReducerCounter.RECORDS_CRUSHED),	equalTo(964L));
	}
	

	@Test
	public void executeIgnoreFile() throws Exception {

		// Ones to include
		writeFile("in_skip_test/file10", Format.TEXT);
		writeFile("in_skip_test/file11", Format.TEXT);
		writeFile("in_skip_test/file12", Format.TEXT);
		// Ones to skip
		writeFile("in_skip_test/file90", Format.TEXT);
		writeFile("in_skip_test/file91", Format.TEXT);
		writeFile("in_skip_test/file92", Format.TEXT);

		Crush crush = new Crush();

		ToolRunner.run(job, crush, new String [] {
			"--threshold=0.015",
			"--max-file-blocks=1",
			"--verbose",
			"--input-format=text",
			"--output-format=text",
			"--compress=none",
			"--ignore-regex=.*9[0-9]",
			
			"in_skip_test", "out_skip_test", "20101116153015"
		});

		verifyOutput(homeDir + "/out_skip_test", "crushed_file-*-*-*", Format.TEXT, Format.TEXT, null, "file10", "file11", "file12");
		
	}

	/**
	 * Copies data from the given input stream to an HDFS file at the given path. This method will close the input stream.
	 */
	protected final void copyStreamToHdfs(InputStream resource, String hdfsDestFileName) throws IOException {
		FileSystem fs = getFileSystem();

		FSDataOutputStream os = fs.create(new Path(hdfsDestFileName), false);

		IOUtils.copyBytes(resource, os, fs.getConf(), true);
	}

	/**
	 * Every file in this unit test is named "file" followed by a number. This method will create a sequence or text file with as
	 * many lines as the number in the file name. The keys in the file will count from one to the number. The values in the file
	 * will count from 100n + 1 to 100n + n. This way each file will have distinct contents so long as no two files have the same
	 * name.
	 */
	private void writeFile(String fileName, Format format) throws IOException {

		int fileNum = Integer.parseInt(fileName.substring(fileName.length() - 2));

		Path path = new Path(fileName);

		if (Format.TEXT == format) {
			PrintWriter writer = new PrintWriter(new BufferedWriter(new PrintWriter(getFileSystem().create(path, false))));

			for (int k = 1, v = 100 * fileNum + 1; k <= fileNum; k++, v++) {
				writer.printf("%d\t%d\n", k, v);
			}

			writer.close();
		} else {
			CustomWritable key = new CustomWritable();
			CustomWritable value = new CustomWritable();

			Writer writer = SequenceFile.createWriter(getFileSystem(), getFileSystem().getConf(), path, CustomWritable.class,
					CustomWritable.class, CompressionType.BLOCK, defaultCodec);

			for (int k = 1, v = 100 * fileNum + 1; k <= fileNum; k++, v++) {
				key.set(k);
				value.set(v);

				writer.append(key, value);
			}

			writer.close();
		}
	}

	/**
	 * Writes the specified number of bytes to the filename. Used to verify that big files are ignored.
	 */
	private void writeHugeFile(String fileName, long length) throws IOException {

		Path path = new Path(fileName);

		OutputStream writer = getFileSystem().create(path, false);

		byte[] bytes = new byte[1024];

		while (length > 0) {
			writer.write(bytes);

			length -= bytes.length;
		}

		writer.close();
	}

	/**
	 * Writes the specified number of bytes to the filename. Used to verify that big files are ignored.
	 */
	private void verifyHugeFile(String fileName, long length) throws IOException {

		Path path = new Path(fileName);

		InputStream is = getFileSystem().open(path);

		byte[] bytes = new byte[1024];
		int read;

		while (-1 != (read = is.read(bytes))) {
			for (int i = 0; i < read; i++) {
				assertThat(bytes[i], equalTo((byte) 0));
			}
		}

		is.close();
	}

	/**
	 * Verifies that the work dir has the expected output.
	 */
	private void verifyOutput(String dir, String crushOutMask, Format inFmt, Format outFmt, CompressionCodec codec, String... fileNames) throws IOException {

		/*
		 * Read format table
		 *
		 *         \   out format
		 *          \
		 * in format \ seq    | text
		 * ----------------------------
		 *      seq  | Custom | ascii |
		 * -------------------------- -
		 *      text | Text   | ascii |
		 * ----------------------------
		 */

		if (Format.TEXT == outFmt) {
			/*
			 * TextInputFormat will produce keys that are byte offsets and values that are the line. This is not actually what we want.
			 * We want to preserve the actual keys and values in the files, just like SequenceFileInputFormat. So, either way, the
			 * keys and values should be the text representations of what went in.
			 */
			BufferedReader reader;
			Path crushOut;

			if (null == codec) {
				Path path = new Path(dir + "/" + crushOutMask);

				FileStatus[] globStatus = getFileSystem().globStatus(path);
				
				if (globStatus == null || 1 != globStatus.length || globStatus[0].isDir()) {
					fail(crushOutMask + " was not found in " + path);
				}

				crushOut = globStatus[0].getPath();

				reader = new BufferedReader(new InputStreamReader(getFileSystem().open(crushOut)));
			} else {
				Path path = new Path(dir + "/" + crushOutMask + codec.getDefaultExtension());

				FileStatus[] globStatus = getFileSystem().globStatus(path);

				if (globStatus == null || 1 != globStatus.length || globStatus[0].isDir()) {
					fail(crushOutMask);
				}

				crushOut = globStatus[0].getPath();

				reader = new BufferedReader(new InputStreamReader(codec.createInputStream(getFileSystem().open(crushOut))));
			}

			Set<String> expected = new HashSet<String>();
			Set<String> actual = new HashSet<String>();

			for (String fileName : fileNames) {
				int max = Integer.parseInt(fileName.substring(4));

				for (int key = 1, value = max * 100 + 1; key <= max; key++, value++) {
					String expectedLine = String.format("%d\t%d", key, value);
					assertThat(expectedLine, expected.add(expectedLine), is(true));

					String actualLine = reader.readLine();
					assertThat(actualLine, actual.add(actualLine), is(true));
				}
			}

			assertThat("Should be at end of crush output file " + crushOut, reader.readLine(), nullValue());

			reader.close();

			assertThat(actual, equalTo(expected));

		} else if (Format.SEQUENCE == inFmt && Format.SEQUENCE == outFmt) {
			/*
			 * Record reader will produce keys that are custom writables and values that are custom writable.
			 */
			FileStatus[] globStatus = getFileSystem().globStatus(new Path(dir + "/" + crushOutMask));

			if (globStatus == null || 1 != globStatus.length || globStatus[0].isDir()) {
				fail(crushOutMask);
			}

			Path crushOut = globStatus[0].getPath();

			Reader reader = new Reader(getFileSystem(), crushOut, getFileSystem().getConf());

			assertThat(reader.isBlockCompressed(), is(true));
			assertThat(reader.getCompressionCodec().getClass(), equalTo((Object) codec.getClass()));

			CustomWritable key = new CustomWritable();
			CustomWritable value = new CustomWritable();

			Set<String> expected = new HashSet<String>();
			Set<String> actual = new HashSet<String>();

			for (String fileName : fileNames) {
				int max = Integer.parseInt(fileName.substring(4));

				for (int k = 1, v = max * 100 + 1; k <= max; k++, v++) {
					reader.next(key, value);

					assertThat(expected.add(String.format("%s\t%s", k, v)), is(true));
					assertThat(actual.add(String.format("%s\t%s", key, value)), is(true));
				}
			}

			assertThat("Should be at end of crush output file " + crushOut, reader.next(key, value), is(false));

			reader.close();

			assertThat(actual, equalTo(expected));

		} else if (Format.TEXT == inFmt && Format.SEQUENCE == outFmt) {

			FileStatus[] globStatus = getFileSystem().globStatus(new Path(dir + "/" + crushOutMask));

			if (globStatus == null || 1 != globStatus.length || globStatus[0].isDir()) {
				fail(crushOutMask);
			}

			Path crushOut = globStatus[0].getPath();

			Reader reader = new Reader(getFileSystem(), crushOut, getFileSystem().getConf());

			assertThat(reader.isCompressed(), is(true));

			assertThat(reader.isBlockCompressed(), is(true));
			assertThat(reader.getCompressionCodec().getClass(), equalTo((Object) codec.getClass()));

			Text key = new Text();
			Text value = new Text();

			Set<String> expected = new HashSet<String>();
			Set<String> actual = new HashSet<String>();

			for (String fileName : fileNames) {
				int max = Integer.parseInt(fileName.substring(4));

				for (int k = 1, v = max * 100 + 1; k <= max; k++, v++) {
					reader.next(key, value);

					assertThat(expected.add(String.format("%s\t%s", k, v)), is(true));
					assertThat(actual.add(String.format("%s\t%s", key, value)), is(true));
				}
			}

			assertThat("Should be at end of crush output file " + crushOut, reader.next(key, value), is(false));

			reader.close();

			assertThat(actual, equalTo(expected));

		} else {
			fail();
		}
	}

	private enum Format {
		TEXT, SEQUENCE
	}

	/**
	 * Makes {@link #before()} and {@link #after()} public so we can invoke them in a static way. We can't use this as a method rule
	 * because we can only set java.io.tmpdir once; we cannot set it before each method.
	 */
	private static class StaticTemporaryFolder extends TemporaryFolder {
		@Override
		public void before() throws Throwable {
			super.before();
		}

		@Override
		public void after() {
			super.after();
		}
	}
}

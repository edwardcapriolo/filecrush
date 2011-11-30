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
import static java.util.Arrays.asList;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.m6d.filecrush.crush.CrushReducer;
import com.m6d.filecrush.crush.ReducerCounter;

@RunWith(Parameterized.class)
@SuppressWarnings("deprecation")
public class CrushReducerParameterizedTest extends EasyMockSupport {
	@Parameters
	public static Collection<Object[]> testCases() {
		List<Object[]> testCases = new ArrayList<Object[]>();

		for (Object[] testCase : new Object[][] {	new Object[] { CompressionType.NONE },
																							new Object[] { CompressionType.BLOCK },
																							new Object[] { CompressionType.RECORD }}) {
			testCases.add(testCase);
		}

		return testCases;
	}

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final CompressionType compressionType;

	private OutputCollector<Text, Text> collector;

	private Reporter reporter;

	private CrushReducer reducer;

	private JobConf job;

	private FileSystem fs;

	/**
	 * Simulates the task attempt work dir that is created by Hadoop.
	 */
	private File workDir;

	/**
	 * Simulates the output dir to which the attempt's output will be copied.
	 */
	private File outDir;

	public CrushReducerParameterizedTest(CompressionType compressionType) {
		super();

		this.compressionType = compressionType;
	}

	@Before
	public void setupReducer() throws IOException {
		job = new JobConf(false);

		job.set("mapred.tip.id", "task_201011081200_014527_r_001234");
		job.set("mapred.task.id", "attempt_201011081200_14527_r_001234_0");

		/*
		 * This logic tree around compression simulates what the output formats do.
		 */
		if (CompressionType.NONE == compressionType) {
			job.setBoolean("mapred.output.compress", false);
		} else {
			job.setBoolean("mapred.output.compress", true);
			job.set("mapred.output.compression.type", compressionType.name());
			job.set("mapred.output.compression.codec", CustomCompressionCodec.class.getName());
		}

		outDir = tmp.newFolder("out");
		tmp.newFolder("out/_temporary");
		workDir = tmp.newFolder("out/_temporary/_" + job.get("mapred.task.id"));

		job.set("mapred.output.dir", outDir.getAbsolutePath());
		job.set("mapred.work.output.dir", workDir.getAbsolutePath());

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

		job.setLong("crush.timestamp", 98765);

		job.setInt("crush.num.specs", 4);
		job.set("crush.0.regex", ".+/other");
		job.set("crush.0.regex.replacement", "${crush.timestamp}-${crush.task.num}-middle-${crush.file.num}-tail");
		job.set("crush.0.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.0.output.format", TextOutputFormat.class.getName());

		job.set("crush.1.regex", ".+/dir");
		job.set("crush.1.regex.replacement", "secondregex-${crush.timestamp}-${crush.task.num}-${crush.file.num}");
		job.set("crush.1.input.format", TextInputFormat.class.getName());
		job.set("crush.1.output.format", SequenceFileOutputFormat.class.getName());

		job.set("crush.2.regex", ".+/dir/([^/]+/)*(.+)");
		job.set("crush.2.regex.replacement", "thirdregex-$2-${crush.timestamp}-${crush.task.num}-${crush.file.num}");
		job.set("crush.2.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.2.output.format", SequenceFileOutputFormat.class.getName());

		job.set("crush.3.regex", ".+/text");
		job.set("crush.3.regex.replacement", "fourthregex-${crush.task.num}-${crush.timestamp}-${crush.file.num}");
		job.set("crush.3.input.format", TextInputFormat.class.getName());
		job.set("crush.3.output.format", TextOutputFormat.class.getName());

		reducer = new CrushReducer();

		reducer.configure(job);

		fs = FileSystem.get(job);
	}

	@Before
	@SuppressWarnings("unchecked")
	public void setupMocks() {
		collector = createMock("collector", OutputCollector.class);
		reporter = createMock("reporter", Reporter.class);
	}

	@Test
	public void reduce() throws IOException {
		reporter.setStatus(isA(String.class));
		expectLastCall().anyTimes();

		/*
		 * We setup a few directories to exercise regexes. In this comment, dirs are distinguished by a trailing slash. The
		 * file name is followed by the bucket id.
		 *
		 * 	dir/
		 * 			file10 0
		 * 			file11 0
		 * 			file12 1
		 * 			file13 1
		 * 			subdir/
		 * 					file20 0
		 * 					file21 0
		 * 					file22 1
		 * 					file23 1
		 * 					file24 1
		 * 					subsubdir/
		 * 							file30 0
		 * 							file31 0
		 * 							file32 0
		 * 							file33 1
		 * 							file34 1
		 * 					other/
		 * 							file40 1
		 * 							file41 1
		 * 							file42 2
		 * 							file43 2
		 * 			other/
		 * 					file50 0
		 * 					file51 0
		 * 					file52 1
		 * 					file53 1
		 * 					file54 3
		 * 					file55 3
		 * 	text/
		 * 			file60 2
		 * 			file61 2
		 * 			file62 3
		 * 			file63 3
		 *
		 * Now setup the dir so the reducer has some data to work with.
		 */

		Map<Text, List<Text>> inputGroups = new LinkedHashMap<Text, List<Text>>();


		/*
		 * 	dir/
		 * 			file10 0
		 * 			file11 0
		 * 			file12 1
		 * 			file13 1
		 *
		 * These files match the first regex.
		 */
		File dir = tmp.newFolder("dir");

		inputGroups.put(new Text(dir.getAbsolutePath() + "-0"), asList(	writeFile(dir, "file10", Format.TEXT),
																																		writeFile(dir, "file11", Format.TEXT)));

		inputGroups.put(new Text(dir.getAbsolutePath() + "-1"), asList(	writeFile(dir, "file12", Format.TEXT),
																																		writeFile(dir, "file13", Format.TEXT)));

		recordCollectForFile(dir, "file10", "secondregex-98765-1234-0");
		recordCollectForFile(dir, "file11", "secondregex-98765-1234-0");
		recordCollectForFile(dir, "file12", "secondregex-98765-1234-1");
		recordCollectForFile(dir, "file13", "secondregex-98765-1234-1");


		/*
		 * dir/
		 * 			subdir/
		 * 					file20 0
		 * 					file21 0
		 * 					file22 1
		 * 					file23 1
		 * 					file24 1
		 */
		File subdir = tmp.newFolder("dir/subdir");

		inputGroups.put(new Text(subdir.getAbsolutePath() + "-0"), asList(	writeFile(subdir, "file20", Format.SEQUENCE),
																																				writeFile(subdir, "file21", Format.SEQUENCE)));

		inputGroups.put(new Text(subdir.getAbsolutePath() + "-1"), asList(	writeFile(subdir, "file22", Format.SEQUENCE),
																																				writeFile(subdir, "file23", Format.SEQUENCE),
																																				writeFile(subdir, "file24", Format.SEQUENCE)));

		recordCollectForFile(subdir, "file20", "thirdregex-subdir-98765-1234-2");
		recordCollectForFile(subdir, "file21", "thirdregex-subdir-98765-1234-2");
		recordCollectForFile(subdir, "file22", "thirdregex-subdir-98765-1234-3");
		recordCollectForFile(subdir, "file23", "thirdregex-subdir-98765-1234-3");
		recordCollectForFile(subdir, "file24", "thirdregex-subdir-98765-1234-3");


		/*
		 * dir/
		 * 			subdir/
		 * 					subsubdir/
		 * 							file30 0
		 * 							file31 0
		 * 							file32 0
		 * 							file33 1
		 * 							file34 1
		 */
		File subsubdir = tmp.newFolder("dir/subdir/subsubdir");

		inputGroups.put(new Text(subsubdir.getAbsolutePath() + "-0"), asList(	writeFile(subsubdir, "file30", Format.SEQUENCE),
																																					writeFile(subsubdir, "file31", Format.SEQUENCE),
																																					writeFile(subsubdir, "file32", Format.SEQUENCE)));

		inputGroups.put(new Text(subsubdir.getAbsolutePath() + "-1"), asList(	writeFile(subsubdir, "file33", Format.SEQUENCE),
																																					writeFile(subsubdir, "file34", Format.SEQUENCE)));

		recordCollectForFile(subsubdir, "file30", "thirdregex-subsubdir-98765-1234-4");
		recordCollectForFile(subsubdir, "file31", "thirdregex-subsubdir-98765-1234-4");
		recordCollectForFile(subsubdir, "file32", "thirdregex-subsubdir-98765-1234-4");
		recordCollectForFile(subsubdir, "file33", "thirdregex-subsubdir-98765-1234-5");
		recordCollectForFile(subsubdir, "file34", "thirdregex-subsubdir-98765-1234-5");


		/*
		 * dir/
		 * 			subdir/
		 * 					other/
		 * 							file40 1
		 * 							file41 1
		 * 							file42 2
		 * 							file43 2
		 */
		File other1 = tmp.newFolder("dir/subdir/other");

		inputGroups.put(new Text(other1.getAbsolutePath() + "-1"), asList(	writeFile(other1, "file40", Format.SEQUENCE),
																																				writeFile(other1, "file41", Format.SEQUENCE)));

		inputGroups.put(new Text(other1.getAbsolutePath() + "-2"), asList(	writeFile(other1, "file42", Format.SEQUENCE),
																																				writeFile(other1, "file43", Format.SEQUENCE)));

		recordCollectForFile(other1, "file40", "98765-1234-middle-6-tail");
		recordCollectForFile(other1, "file41", "98765-1234-middle-6-tail");
		recordCollectForFile(other1, "file42", "98765-1234-middle-7-tail");
		recordCollectForFile(other1, "file43", "98765-1234-middle-7-tail");


		/*
		 * dir/
		 * 			other/
		 * 					file50 0
		 * 					file51 0
		 * 					file52 1
		 * 					file53 1
		 * 					file54 3
		 * 					file55 3
		 */
		File other2 = tmp.newFolder("dir/other");

		inputGroups.put(new Text(other2.getAbsolutePath() + "-0"), asList(	writeFile(other2, "file50", Format.SEQUENCE),
																																				writeFile(other2, "file51", Format.SEQUENCE)));

		inputGroups.put(new Text(other2.getAbsolutePath() + "-1"), asList(	writeFile(other2, "file52", Format.SEQUENCE),
																																				writeFile(other2, "file53", Format.SEQUENCE)));

		inputGroups.put(new Text(other2.getAbsolutePath() + "-3"), asList(	writeFile(other2, "file54", Format.SEQUENCE),
																																				writeFile(other2, "file55", Format.SEQUENCE)));

		recordCollectForFile(other2, "file50", "98765-1234-middle-8-tail");
		recordCollectForFile(other2, "file51", "98765-1234-middle-8-tail");
		recordCollectForFile(other2, "file52", "98765-1234-middle-9-tail");
		recordCollectForFile(other2, "file53", "98765-1234-middle-9-tail");
		recordCollectForFile(other2, "file54", "98765-1234-middle-10-tail");
		recordCollectForFile(other2, "file55", "98765-1234-middle-10-tail");

		/*
		 * 	text/
		 * 			file60 2
		 * 			file61 2
		 * 			file62 3
		 * 			file63 3
		 */
		File text = tmp.newFolder("text");

		inputGroups.put(new Text(text.getAbsolutePath() + "-2"), asList(writeFile(text, "file60", Format.TEXT),
																																		writeFile(text, "file61", Format.TEXT)));

		inputGroups.put(new Text(text.getAbsolutePath() + "-3"), asList(writeFile(text, "file62", Format.TEXT),
																																		writeFile(text, "file63", Format.TEXT)));

		recordCollectForFile(text, "file60", "fourthregex-1234-98765-11");
		recordCollectForFile(text, "file61", "fourthregex-1234-98765-11");
		recordCollectForFile(text, "file62", "fourthregex-1234-98765-12");
		recordCollectForFile(text, "file63", "fourthregex-1234-98765-12");

		replayAll();

		for (Entry<Text, List<Text>> e : inputGroups.entrySet()) {
			reducer.reduce(e.getKey(), e.getValue().iterator(), collector, reporter);
		}

		verifyAll();

		verifyWorkOutput(dir,				"secondregex-98765-1234-0",						Format.TEXT,			Format.SEQUENCE, "file10", "file11");
		verifyWorkOutput(dir,				"secondregex-98765-1234-1",						Format.TEXT,			Format.SEQUENCE, "file12", "file13");
		verifyWorkOutput(subdir,		"thirdregex-subdir-98765-1234-2",			Format.SEQUENCE,	Format.SEQUENCE, "file20", "file21");
		verifyWorkOutput(subdir,		"thirdregex-subdir-98765-1234-3",			Format.SEQUENCE,	Format.SEQUENCE, "file22", "file23", "file24");
		verifyWorkOutput(subsubdir, "thirdregex-subsubdir-98765-1234-4",	Format.SEQUENCE,	Format.SEQUENCE, "file30", "file31", "file32");
		verifyWorkOutput(subsubdir, "thirdregex-subsubdir-98765-1234-5",	Format.SEQUENCE,	Format.SEQUENCE, "file33", "file34");
		verifyWorkOutput(other1,		"98765-1234-middle-6-tail",						Format.SEQUENCE,	Format.TEXT, "file40", "file41");
		verifyWorkOutput(other1,		"98765-1234-middle-7-tail",						Format.SEQUENCE,	Format.TEXT, "file42", "file43");
		verifyWorkOutput(other2,		"98765-1234-middle-8-tail",						Format.SEQUENCE,	Format.TEXT, "file50", "file51");
		verifyWorkOutput(other2,		"98765-1234-middle-9-tail",						Format.SEQUENCE,	Format.TEXT, "file52", "file53");
		verifyWorkOutput(other2,		"98765-1234-middle-10-tail",					Format.SEQUENCE,	Format.TEXT, "file54", "file55");
		verifyWorkOutput(text,			"fourthregex-1234-98765-11",					Format.TEXT,			Format.TEXT, "file60", "file61");
		verifyWorkOutput(text,			"fourthregex-1234-98765-12",					Format.TEXT,			Format.TEXT, "file62", "file63");
	}

	/**
	 * Verifies that the work dir has the expected output.
	 */
	private void verifyWorkOutput(File srcDir, String crushedOutFileName, Format inFmt, Format outFmt, String... fileNames) throws IOException {

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
		File crushOutput = new File(workDir.getAbsolutePath() + "/crush" + srcDir.getAbsolutePath() + "/" + crushedOutFileName);

		if (Format.TEXT == outFmt) {
			/*
			 * TextInputFormat will produce keys that are byte offsets and values that are the line. This is not actually what we want.
			 * We want to preserve the actualy keys and values in the files, just like SequenceFileInputFormat. So, either way, the
			 * keys and values should be the text representations of what went in.
			 */
			BufferedReader reader;

			/*
			 * Text output format appends the default extension of the codec, if there is one.
			 */
			if (CompressionType.NONE == compressionType) {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(crushOutput)));
			} else {
				CustomCompressionCodec codec = new CustomCompressionCodec();
				codec.setConf(job);

				reader = new BufferedReader(new InputStreamReader(codec.createInputStream(new FileInputStream(crushOutput + ".custom"))));
			}

			String line = "";

			for (String fileName : fileNames) {
				int max = Integer.parseInt(fileName.substring(4));

				for (int key = 1, value = max * 100 + 1; key <= max; key++, value++) {
					String expectedLine = format("%d\t%d", key, value);

					line = reader.readLine();

					assertThat(line, equalTo(expectedLine));
				}
			}

			assertThat("Should be at end of crush output file" + crushedOutFileName, reader.readLine(), nullValue());

			reader.close();
		} else if (Format.SEQUENCE == inFmt && Format.SEQUENCE == outFmt) {
			/*
			 * Record reader will produce keys that are custom writables and values that are custom writable.
			 */
			Reader reader = new Reader(fs, new Path(crushOutput.getAbsolutePath()), job);

			assertThat(reader.isCompressed(), is(compressionType != CompressionType.NONE));

			if (reader.isCompressed()) {
				assertThat(reader.isBlockCompressed(), is(compressionType == CompressionType.BLOCK));
				assertThat(reader.getCompressionCodec().getClass(), equalTo((Object) CustomCompressionCodec.class));
			}

			CustomWritable key = new CustomWritable();
			CustomWritable value = new CustomWritable();

			for (String fileName : fileNames) {
				int max = Integer.parseInt(fileName.substring(4));

				for (int k = 1, v = max * 100 + 1; k <= max; k++, v++) {
					reader.next(key, value);

					assertThat(fileName, key.get(), equalTo((long) k));
					assertThat(fileName, value.get(), equalTo((long) v));
				}
			}

			assertThat("Should be at end of crush output file" + crushedOutFileName, reader.next(key, value), is(false));

			reader.close();
		} else if (Format.TEXT == inFmt && Format.SEQUENCE == outFmt) {

			Reader reader = new Reader(fs, new Path(crushOutput.getAbsolutePath()), job);

			assertThat(reader.isCompressed(), is(compressionType != CompressionType.NONE));

			if (reader.isCompressed()) {
				assertThat(reader.isBlockCompressed(), is(compressionType == CompressionType.BLOCK));
				assertThat(reader.getCompressionCodec().getClass(), equalTo((Object) CustomCompressionCodec.class));
			}

			Text key = new Text();
			Text value = new Text();

			for (String fileName : fileNames) {
				int max = Integer.parseInt(fileName.substring(4));

				for (int k = 1, v = max * 100 + 1; k <= max; k++, v++) {
					reader.next(key, value);

					assertThat(fileName, key.toString(), equalTo(Integer.toString(k)));
					assertThat(fileName, value.toString(), equalTo(Integer.toString(v)));
				}
			}

			assertThat("Should be at end of crush output file" + crushedOutFileName, reader.next(key, value), is(false));

			reader.close();
		} else {
			fail();
		}
	}

	/**
	 * Records an expectation that a file has been crushed. The key is the absolute path of the crush input file. The value is the
	 * absolute path of the crush output file, which is rooted in the output dir/crush (<b>not</b> the attempt work dir).
	 */
	private void recordCollectForFile(File srcDir, String crushInput, String crushOutput) throws IOException {
		Text srcFileAbsPath = new Text(new File(srcDir, crushInput).getAbsolutePath());
		Text fileInJobOutputDir = new Text(format("%s/crush%s", outDir.getAbsolutePath(), new File(srcDir, crushOutput).getAbsolutePath()));

		collector.collect(srcFileAbsPath, fileInJobOutputDir);
		reporter.incrCounter(ReducerCounter.FILES_CRUSHED, 1);

		reporter.incrCounter(ReducerCounter.RECORDS_CRUSHED, 1);
		expectLastCall().times(Integer.parseInt(crushInput.substring(4)));
	}

	/**
	 * Every file in this unit test is named "file" followed by a number. This method will create a sequence file with as many lines
	 * as the number in the file name. The keys in the file will count from one to the number. The values in the file will count
	 * from 100n + 1 to 100n + n. This way each file will have distinct contents so long as no two files have the same name.
	 */
	private Text writeFile(File srcDir, String fileName, Format format) throws IOException {

		int fileNum = Integer.parseInt(fileName.substring(4));

		File file = new File(srcDir, fileName);

		if (Format.TEXT == format) {
			PrintWriter writer = new PrintWriter(new FileOutputStream(file));

			for (int k = 1, v = 100 * fileNum + 1; k <= fileNum; k++, v++) {
				writer.printf("%d\t%d\n", k, v);
			}

			writer.close();
		} else {
			CustomWritable key = new CustomWritable();
			CustomWritable value = new CustomWritable();

			DefaultCodec codec = new DefaultCodec();
			codec.setConf(job);

			Writer writer = SequenceFile.createWriter(fs, job, new Path(file.getAbsolutePath()), CustomWritable.class,
					CustomWritable.class, compressionType, codec);

			for (int k = 1, v = 100 * fileNum + 1; k <= fileNum; k++, v++) {
				key.set(k);
				value.set(v);

				writer.append(key, value);
			}

			writer.close();
		}

		return new Text(file.getAbsolutePath());
	}

	private enum Format {
		TEXT, SEQUENCE
	}

	/**
	 * This only sexists to prove that the reducer can read and write custom writables about which it has no a priori knowledge.
	 */
	public static class CustomWritable extends LongWritable {
	}

	/**
	 * This only exists to prove that the reducer can use custom codecs.
	 */
	public static class CustomCompressionCodec extends DefaultCodec {
		public CustomCompressionCodec() {
			super();
		}

		@Override
		public String getDefaultExtension() {
			return ".custom";
		}
	}
}

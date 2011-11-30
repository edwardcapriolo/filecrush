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

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.m6d.filecrush.crush.Crush;

@SuppressWarnings("deprecation")
public class CrushOptionParsingTest {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private Crush crush;

	@Before
	public void before() throws IOException {
		crush = new Crush();

		JobConf job = new JobConf(false);
		crush.setConf(job);

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.setInt("mapred.reduce.tasks", 20);
		job.setLong("dfs.block.size", 1024 * 1024 * 64);

		FileSystem fs = FileSystem.get(job);
		fs.setWorkingDirectory(new Path(tmp.getRoot().getAbsolutePath()));

		crush.setFileSystem(fs);
	}

	@Test
	public void unrecognizedOption() {
		try {
			crush.createJobConfAndParseArgs("-bad", "in", "out", "20101116123015");
			fail();
		} catch (Exception e) {
		}
	}

	@Test
	public void badRegexCount() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--replacement", "bar",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().equals("Must be an equal number of regex, replacement, in-format, and out-format options")) {
				throw e;
			}
		}
	}

	@Test
	public void badCompressCodec() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--compress", "java.lang.Object",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("java.lang.Object")) {
				throw e;
			}
		}
	}

	@Test
	public void badCompressCodecNotAClass() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--compress", "foo",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("foo")) {
				throw e;
			}
		}
	}

	@Test
	public void badReplacementCount() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--regex", ".+/act/.+",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().equals("Must be an equal number of regex, replacement, in-format, and out-format options")) {
				throw e;
			}
		}
	}

	@Test
	public void badInputFormatCount() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--regex", ".+/act/.+",
					"--replacement", "bar",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().equals("Must be an equal number of regex, replacement, in-format, and out-format options")) {
				throw e;
			}
		}
	}

	@Test
	public void badOutputFormatCount() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--regex", ".+/act/.+",
					"--replacement", "bar",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().equals("Must be an equal number of regex, replacement, in-format, and out-format options")) {
				throw e;
			}
		}
	}

	@Test
	public void badInputFormat() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--regex", ".+/act/.+",
					"--replacement", "bar",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.SequenceFileOutputFormat",
					"--regex", ".+/bid/.+",
					"--replacement", "hello",
					"--input-format", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--threshold", "0.5",
					"--max-file-blocks", "100",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("org.apache.hadoop.mapreduce.lib.input.TextInputFormat")) {
				throw e;
			}
		}
	}

	@Test
	public void badInputFormatNotAClass() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--regex", ".+/act/.+",
					"--replacement", "bar",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.SequenceFileOutputFormat",
					"--regex", ".+/bid/.+",
					"--replacement", "hello",
					"--input-format", "foo",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--threshold", "0.5",
					"--max-file-blocks", "100",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("foo")) {
				throw e;
			}
		}
	}

	@Test
	public void badOutputFormat() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--regex", ".+/act/.+",
					"--replacement", "bar",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.SequenceFileOutputFormat",
					"--regex", ".+/bid/.+",
					"--replacement", "hello",
					"--input-format", "org.apache.hadoop.mapred.SequenceFileInputFormat",
					"--output-format", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
					"--threshold", "0.5",
					"--max-file-blocks", "100",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")) {
				throw e;
			}
		}
	}

	@Test
	public void badOutputFormatNotAClass() throws Exception {
		try {
			crush.createJobConfAndParseArgs(
					"--regex", ".+/ads/.+",
					"--replacement", "foo",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
					"--regex", ".+/act/.+",
					"--replacement", "bar",
					"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
					"--output-format", "org.apache.hadoop.mapred.SequenceFileOutputFormat",
					"--regex", ".+/bid/.+",
					"--replacement",	"hello",
					"--input-format", "org.apache.hadoop.mapred.SequenceFileInputFormat",
					"--output-format", "foo",
					"--threshold", "0.5",
					"--max-file-blocks", "100",
					"in", "out", "20101116123015");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("foo")) {
				throw e;
			}
		}
	}

	@Test
	public void badSourceDir() throws Exception {
		try {
			crush.createJobConfAndParseArgs("does not exist", tmp.newFolder("out").getAbsolutePath(), "20101116123015");
		} catch (IOException e) {
			if (!e.getMessage().contains("does not exist")) {
				throw e;
			}
		}
	}

	@Test
	public void defaults() throws Exception {
		crush.createJobConfAndParseArgs(tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "20101116123015");

		JobConf job = crush.getJob();

		assertThat(job.get("mapred.reduce.tasks"), equalTo("20"));
		assertThat(job.get("mapred.output.compress"), equalTo("true"));
		assertThat(job.get("mapred.output.compression.type"), equalTo("BLOCK"));
		assertThat(job.get("mapred.output.compression.codec"), equalTo("org.apache.hadoop.io.compress.DefaultCodec"));

		assertThat(crush.getMaxFileBlocks(), equalTo(8));

		assertThat(job.get("crush.timestamp"), equalTo("20101116123015"));

		assertThat(job.get("crush.num.specs"), equalTo("1"));

		assertThat(job.get("crush.0.regex"), equalTo(".+"));
		assertThat(job.get("crush.0.regex.replacement"), equalTo("crushed_file-20101116123015-${crush.task.num}-${crush.file.num}"));
		assertThat(job.get("crush.0.input.format"), equalTo("org.apache.hadoop.mapred.SequenceFileInputFormat"));
		assertThat(job.get("crush.0.output.format"), equalTo("org.apache.hadoop.mapred.SequenceFileOutputFormat"));
	}

	@Test
	public void disableCompression() throws Exception {
		crush.createJobConfAndParseArgs(
				"--compress=none",
				tmp.newFolder("in").getAbsolutePath(),
				tmp.newFolder("out").getAbsolutePath(),
				"20101116123015");

		JobConf job = crush.getJob();

		assertThat(job.get("mapred.reduce.tasks"), equalTo("20"));
		assertThat(job.get("mapred.output.compress"), equalTo("false"));

		assertThat(crush.getMaxFileBlocks(), equalTo(8));

		assertThat(job.get("crush.timestamp"), equalTo("20101116123015"));

		assertThat(job.get("crush.num.specs"), equalTo("1"));

		assertThat(job.get("crush.0.regex"), equalTo(".+"));
		assertThat(job.get("crush.0.regex.replacement"), equalTo("crushed_file-20101116123015-${crush.task.num}-${crush.file.num}"));
		assertThat(job.get("crush.0.input.format"), equalTo("org.apache.hadoop.mapred.SequenceFileInputFormat"));
		assertThat(job.get("crush.0.output.format"), equalTo("org.apache.hadoop.mapred.SequenceFileOutputFormat"));
	}

	@Test
	public void parse() throws Exception {
		crush.createJobConfAndParseArgs(
				"--regex", ".+/ads/.+",
				"--replacement", "foo",
				"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
				"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
				"--regex", ".+/act/.+",
				"--replacement", "bar",
				"--input-format", "org.apache.hadoop.mapred.TextInputFormat",
				"--output-format", "org.apache.hadoop.mapred.SequenceFileOutputFormat",
				"--regex", ".+/bid/.+",
				"--replacement", "hello",
				"--input-format", "org.apache.hadoop.mapred.SequenceFileInputFormat",
				"--output-format", "org.apache.hadoop.mapred.TextOutputFormat",
				"--threshold", "0.5",
				"--max-file-blocks", "100",
				"--compress", "org.apache.hadoop.io.compress.DefaultCodec",

				tmp.newFolder("in").getAbsolutePath(), tmp.newFolder("out").getAbsolutePath(), "20101116123015");

		JobConf job = crush.getJob();

		assertThat(job.get("mapred.reduce.tasks"), equalTo("20"));
		assertThat(job.get("mapred.output.compress"), equalTo("true"));
		assertThat(job.get("mapred.output.compression.codec"), equalTo("org.apache.hadoop.io.compress.DefaultCodec"));

		assertThat(crush.getMaxFileBlocks(), equalTo(100));

		assertThat(job.get("crush.timestamp"), equalTo("20101116123015"));

		assertThat(job.get("crush.num.specs"), equalTo("3"));

		assertThat(job.get("crush.0.regex"), equalTo(".+/ads/.+"));
		assertThat(job.get("crush.0.regex.replacement"), equalTo("foo"));
		assertThat(job.get("crush.0.input.format"), equalTo("org.apache.hadoop.mapred.TextInputFormat"));
		assertThat(job.get("crush.0.output.format"), equalTo("org.apache.hadoop.mapred.TextOutputFormat"));

		assertThat(job.get("crush.1.regex"), equalTo(".+/act/.+"));
		assertThat(job.get("crush.1.regex.replacement"), equalTo("bar"));
		assertThat(job.get("crush.1.input.format"), equalTo("org.apache.hadoop.mapred.TextInputFormat"));
		assertThat(job.get("crush.1.output.format"), equalTo("org.apache.hadoop.mapred.SequenceFileOutputFormat"));

		assertThat(job.get("crush.2.regex"), equalTo(".+/bid/.+"));
		assertThat(job.get("crush.2.regex.replacement"), equalTo("hello"));
		assertThat(job.get("crush.2.input.format"), equalTo("org.apache.hadoop.mapred.SequenceFileInputFormat"));
		assertThat(job.get("crush.2.output.format"), equalTo("org.apache.hadoop.mapred.TextOutputFormat"));
	}

	@Test
	public void parseOldNoType() throws Exception {
		long millis = currentTimeMillis();

		crush.createJobConfAndParseArgs(
				tmp.newFolder("in").getAbsolutePath(),
				tmp.newFolder("out").getAbsolutePath(),
				"80");

		JobConf job = crush.getJob();

		assertThat(job.get("mapred.reduce.tasks"), equalTo("80"));
		assertThat(Long.parseLong(job.get("crush.timestamp")), greaterThanOrEqualTo(millis));
		assertThat(job.get("crush.num.specs"), equalTo("1"));

		assertThat(crush.getMaxFileBlocks(), equalTo(Integer.MAX_VALUE));

		assertThat(job.get("crush.0.regex"), equalTo(".+"));
		assertThat(job.get("crush.0.regex.replacement").matches("crushed_file-\\d+-\\$\\{crush.task.num\\}-\\$\\{crush.file.num\\}"), is(true));
		assertThat(job.get("crush.0.input.format"), equalTo("org.apache.hadoop.mapred.SequenceFileInputFormat"));
		assertThat(job.get("crush.0.output.format"), equalTo("org.apache.hadoop.mapred.SequenceFileOutputFormat"));
	}

	@Test
	public void parseOldSequence() throws Exception {
		long millis = currentTimeMillis();

		crush.createJobConfAndParseArgs(
				tmp.newFolder("in").getAbsolutePath(),
				tmp.newFolder("out").getAbsolutePath(),
				"80",
				"SEQUENCE");

		JobConf job = crush.getJob();

		assertThat(job.get("mapred.reduce.tasks"), equalTo("80"));
		assertThat(Long.parseLong(job.get("crush.timestamp")), greaterThanOrEqualTo(millis));
		assertThat(job.get("crush.num.specs"), equalTo("1"));

		assertThat(crush.getMaxFileBlocks(), equalTo(Integer.MAX_VALUE));

		assertThat(job.get("crush.0.regex"), equalTo(".+"));
		assertThat(job.get("crush.0.regex.replacement").matches("crushed_file-\\d+-\\$\\{crush.task.num\\}-\\$\\{crush.file.num\\}"), is(true));
		assertThat(job.get("crush.0.input.format"), equalTo("org.apache.hadoop.mapred.SequenceFileInputFormat"));
		assertThat(job.get("crush.0.output.format"), equalTo("org.apache.hadoop.mapred.SequenceFileOutputFormat"));
	}

	@Test
	public void parseOldText() throws Exception {
		long millis = currentTimeMillis();

		crush.createJobConfAndParseArgs(
				tmp.newFolder("in").getAbsolutePath(),
				tmp.newFolder("out").getAbsolutePath(),
				"80",
				"TEXT");

		JobConf job = crush.getJob();

		assertThat(job.get("mapred.reduce.tasks"), equalTo("80"));
		assertThat(Long.parseLong(job.get("crush.timestamp")), greaterThanOrEqualTo(millis));
		assertThat(job.get("crush.num.specs"), equalTo("1"));

		assertThat(crush.getMaxFileBlocks(), equalTo(Integer.MAX_VALUE));

		assertThat(job.get("crush.0.regex"), equalTo(".+"));
		assertThat(job.get("crush.0.regex.replacement").matches("crushed_file-\\d+-\\$\\{crush.task.num\\}-\\$\\{crush.file.num\\}"), is(true));
		assertThat(job.get("crush.0.input.format"), equalTo("org.apache.hadoop.mapred.TextInputFormat"));
		assertThat(job.get("crush.0.output.format"), equalTo("org.apache.hadoop.mapred.TextOutputFormat"));
	}

	@Test
	public void parseOldBadType() throws Exception {
		try {
			crush.createJobConfAndParseArgs("in",
					"out",
					"80",
					"FOO");
			fail();
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("FOO")) {
				throw e;
			}
		}
	}
}

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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.m6d.filecrush.crush.Crush;

/**
 * Dfs block size will be set to 50 and threshold set to 20%.
 */
@SuppressWarnings("deprecation")
public class CrushStandAloneTextTest {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private JobConf job;

	@Before
	public void setup() throws Exception {
		job = new JobConf(false);

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.setLong("dfs.block.size", 50);
	}

	/**
	 * Crush creates a subdirectory in tmp to store all its transient data. Since this test uses the local file system, the present
	 * working directory is the parent of tmp. We delete it here since it's not so useful to clutter the build directory with
	 * empty directories.
	 */
	@After
	public void deleteTmp() throws IOException {
		File tmp = new File("tmp");

		if (tmp.exists()) {
			assertThat(tmp.delete(), is(true));
		}
	}

	@Test
	public void standAloneOutput() throws Exception {

		File in = tmp.newFolder("in");

		createFile(in, "skipped-0", 0, 25);
		createFile(in, "skipped-1", 1, 25);
		createFile(in, "skipped-2", 2, 25);
		createFile(in, "skipped-3", 3, 25);

		File subdir = tmp.newFolder("in/subdir");

		createFile(subdir, "lil-0", 0, 1);
		createFile(subdir, "lil-1", 1, 2);
		createFile(subdir, "big-2", 2, 5);
		createFile(subdir, "big-3", 3, 5);

		File subsubdir = tmp.newFolder("in/subdir/subsubdir");

		createFile(subsubdir, "skipped-4", 4, 25);
		createFile(subsubdir, "skipped-5", 5, 25);

		File out = new File(tmp.getRoot(), "out");

		ToolRunner.run(job, new Crush(), new String[] {
				"--input-format=text",
				"--output-format=text",
				"--compress=none",

				subdir.getAbsolutePath(), out.getAbsolutePath()
		});

		/*
		 * Make sure the original files are still there.
		 */
		verifyFile(in, "skipped-0", 0, 25);
		verifyFile(in, "skipped-1", 1, 25);
		verifyFile(in, "skipped-2", 2, 25);
		verifyFile(in, "skipped-3", 3, 25);

		verifyFile(subdir, "lil-0", 0, 1);
		verifyFile(subdir, "lil-1", 1, 2);
		verifyFile(subdir, "big-2", 2, 5);
		verifyFile(subdir, "big-3", 3, 5);

		verifyFile(subsubdir, "skipped-4", 4, 25);
		verifyFile(subsubdir, "skipped-5", 5, 25);

		/*
		 * Verify the crush output.
		 */
		verifyCrushOutput(out, new int[] { 0, 1 }, new int[] { 1, 2}, new int[] { 2, 5 }, new int[] { 3, 5 });
	}

	@Test
	public void noFiles() throws Exception {
		File in = tmp.newFolder("in");

		File out = new File(tmp.getRoot(), "out");

		ToolRunner.run(job, new Crush(), new String[] {
				in.getAbsolutePath(), out.getAbsolutePath()
		});

		assertThat(out.exists(), is(false));
	}

	@Test
	public void ignoreRegexTest() throws Exception {

		File in = tmp.newFolder("skip_test");

		createFile(in, "lil-0", 0, 1);
		createFile(in, "lil-1", 1, 2);
		createFile(in, "big-2", 2, 5);
		createFile(in, "big-3", 3, 5);
		// Files to be ignored
		createFile(in, "lil-0.index", 0, 10);
		createFile(in, "lil-1.index", 1, 20);
		createFile(in, "big-2.index", 2, 50);
		createFile(in, "big-3.index", 3, 50);

		File out = new File(tmp.getRoot(), "skip_test_out");

		ToolRunner.run(job, new Crush(), new String[] {
				"--input-format=text",
				"--output-format=text",
				"--ignore-regex=.*\\.index",
				"--compress=none",

				in.getAbsolutePath(), out.getAbsolutePath()
		});

		/*
		 * Make sure the original files are still there.
		 */
		verifyFile(in, "lil-0", 0, 1);
		verifyFile(in, "lil-1", 1, 2);
		verifyFile(in, "big-2", 2, 5);
		verifyFile(in, "big-3", 3, 5);
		verifyFile(in, "lil-0.index", 0, 10);
		verifyFile(in, "lil-1.index", 1, 20);
		verifyFile(in, "big-2.index", 2, 50);
		verifyFile(in, "big-3.index", 3, 50);

		/*
		 * Verify the crush output.
		 */
		verifyCrushOutput(out, new int[] { 0, 1 }, new int[] { 1, 2}, new int[] { 2, 5 }, new int[] { 3, 5 });
	}

	private void verifyCrushOutput(File crushOutput, int[]... keyCounts) throws IOException {

		List<String> actual = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(crushOutput));

		String line;

		while (null != (line = reader.readLine())) {
			actual.add(line);
		}

		reader.close();

		int expLines = 0;
		List<List<String>> expected = new ArrayList<List<String>>();

		for (int[] kc : keyCounts) {
			int key  = kc[0];
			int count = kc[1];

			List<String> lines = new ArrayList<String>();
			expected.add(lines);

			for (int idx = 0, i = 0; idx < count; idx++, i = i == 9 ? 0 : i + 1) {
				line = format("%d\t%d", key, i);
				lines.add(line);
			}

			expLines += count;
		}

		/*
		 * Make sure each file's data is contiguous in the crush output file.
		 */
		for (List<String> list : expected) {
			int idx = actual.indexOf(list.get(0));

			assertThat(idx, greaterThanOrEqualTo(0));

			assertThat(actual.subList(idx, idx + list.size()), equalTo(list));
		}

		assertThat(actual.size(), equalTo(expLines));
	}

	private void createFile(File dir, String fileName, int key, int count) throws IOException {
		File file = new File(dir, fileName);

		PrintWriter writer = new PrintWriter(file);

		for (int idx = 0, i = 0; idx < count; idx++, i = i == 9 ? 0 : i + 1) {
			String line = format("%d\t%d\n", key, i);

			assertThat(line.length(), equalTo(4));

			writer.write(line);
		}

		writer.close();
	}

	private void verifyFile(File dir, String fileName, int key, int count) throws IOException {
		File file = new File(dir, fileName);

		assertThat(file.isFile(), is(true));
		assertThat(file.length(), equalTo((long) count * 4));

		BufferedReader reader = new BufferedReader(new FileReader(file));

		String line;
		int i = 0;
		int actualCount = 0;

		while (null != (line = reader.readLine())) {
			assertThat(line.length(), equalTo(3));

			actualCount++;

			String[] split = line.split("\t");

			assertThat(line, split[0], equalTo(Integer.toString(key)));
			assertThat(line, split[1], equalTo(Integer.toString(i)));

			if (i == 9) {
				i = 0;
			} else {
				i++;
			}
		}

		assertThat(reader.readLine(), nullValue());

		reader.close();

		assertThat(actualCount, equalTo(count));
	}
}

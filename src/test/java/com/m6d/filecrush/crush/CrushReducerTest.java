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

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.m6d.filecrush.crush.CrushReducer;
import com.m6d.filecrush.crush.KeyValuePreservingTextInputFormat;

@SuppressWarnings("deprecation")
public class CrushReducerTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private File outDir;

	private CrushReducer reducer;

	@Before
	public void setupReducer() {
		JobConf job = new JobConf(false);

		job.set("mapred.tip.id", "task_201011081200_014527_r_001234");
		job.set("mapred.task.id", "attempt_201011081200_14527_r_001234_0");

		outDir = tmp.newFolder("out");
		tmp.newFolder("out/_temporary");

		job.set("mapred.output.dir", outDir.getAbsolutePath());

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

		job.setLong("crush.timestamp", 98765);

		job.setInt("crush.num.specs", 3);
		job.set("crush.0.regex", ".+/dir");
		job.set("crush.0.regex.replacement", "firstregex-${crush.timestamp}-${crush.task.num}-${crush.file.num}");
		job.set("crush.0.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.0.output.format", TextOutputFormat.class.getName());

		job.set("crush.1.regex", ".+/dir/([^/]+/)*(.+)");
		job.set("crush.1.regex.replacement", "secondregex-$2-${crush.timestamp}-${crush.task.num}-${crush.file.num}");
		job.set("crush.1.input.format", TextInputFormat.class.getName());
		job.set("crush.1.output.format", TextOutputFormat.class.getName());

		job.set("crush.2.regex", ".+/other");
		job.set("crush.2.regex.replacement", "${crush.timestamp}-${crush.task.num}-middle-${crush.file.num}-tail");
		job.set("crush.2.input.format", TextInputFormat.class.getName());
		job.set("crush.2.output.format", SequenceFileOutputFormat.class.getName());

		reducer = new CrushReducer();

		reducer.configure(job);
	}

	@Test
	public void taskNum() {
		assertThat("task_201011081200_14527_r_1234 => 1234", reducer.getTaskNum(), equalTo(1234));
	}

	@Test
	public void timestamp() {
		assertThat(reducer.getTimestamp(), equalTo(98765L));
	}

	@Test
	public void inputRegexList() {
		assertThat(reducer.getInputRegexList(), equalTo(asList(".+/dir", ".+/dir/([^/]+/)*(.+)", ".+/other")));
	}

	@Test
	public void outputReplacementList() {
		/*
		 * Job configuration already performs some token substitution.
		 */
		assertThat(reducer.getOutputReplacementList(), equalTo(asList("firstregex-98765-${crush.task.num}-${crush.file.num}",
																																	"secondregex-$2-98765-${crush.task.num}-${crush.file.num}",
																																	"98765-${crush.task.num}-middle-${crush.file.num}-tail")));
	}

	@Test
	public void inputFormatList() {
		assertThat(reducer.getInputFormatList(), equalTo(Arrays.<Class<?>> asList(SequenceFileInputFormat.class,
																																							KeyValuePreservingTextInputFormat.class,
																																							KeyValuePreservingTextInputFormat.class)));
	}

	@Test
	public void outputFormatList() {
		assertThat(reducer.getOutputFormatList(), equalTo(Arrays.<Class<?>> asList(	TextOutputFormat.class,
																																								TextOutputFormat.class,
																																								SequenceFileOutputFormat.class)));
	}

	@Test
	public void calculateOutputfile() {
		assertThat(reducer.findMatcher("/path/to/a/dir"), equalTo(0));
		assertThat(reducer.calculateOutputFile(0, "/path/to/a/dir"), equalTo("/path/to/a/dir/firstregex-98765-1234-0"));

		assertThat(reducer.findMatcher("/path/to/a/dir/foo/dir"), equalTo(0));
		assertThat(reducer.calculateOutputFile(0, "/path/to/a/dir/foo/dir"), equalTo("/path/to/a/dir/foo/dir/firstregex-98765-1234-1"));

		assertThat(reducer.findMatcher("/path/to/a/dir/subdir"), equalTo(1));
		assertThat(reducer.calculateOutputFile(1, "/path/to/a/dir/subdir"), equalTo("/path/to/a/dir/subdir/secondregex-subdir-98765-1234-2"));

		assertThat(reducer.findMatcher("/x/dir/foo/bar"), equalTo(1));
		assertThat(reducer.calculateOutputFile(1, "/x/dir/foo/bar"), equalTo("/x/dir/foo/bar/secondregex-bar-98765-1234-3"));

		assertThat(reducer.findMatcher("/x/other"), equalTo(2));
		assertThat(reducer.calculateOutputFile(2, "/x/other"), equalTo("/x/other/98765-1234-middle-4-tail"));

		assertThat(reducer.findMatcher("/x/foo/other"), equalTo(2));
		assertThat(reducer.calculateOutputFile(2, "/x/foo/other"), equalTo("/x/foo/other/98765-1234-middle-5-tail"));
	}

	@Test
	public void fileNotFound() throws IOException {
		try {
			reducer.reduce(new Text("/path/to/a/dir-4"), asList(new Text("/file/does/not/exist")).iterator(), null, null);
			fail();
		} catch (IOException e) {
			if (!e.getMessage().contains("/file/does/not/exist")) {
				throw e;
			}
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void noMatchingInputPattern() {
		reducer.findMatcher("nothing matches me");
	}

	@Test
	public void missingInputRegex() {
		JobConf job = new JobConf(false);

		job.set("mapred.tip.id", "task_201011081200_14527_r_1234");

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.set("mapred.output.dir", outDir.getAbsolutePath());

		job.setLong("crush.timestamp", 98765);

		job.setLong("dfs.block.size", 1024 * 1024 * 64L);

		job.setInt("crush.num.specs", 2);
		job.set("crush.0.regex", "foo");
		job.set("crush.0.regex.replacement", "bar");
		job.set("crush.0.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.0.output.format", TextOutputFormat.class.getName());

		job.set("crush.1.regex.replacement", "bar");
		job.set("crush.1.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.1.output.format", TextOutputFormat.class.getName());

		reducer = new CrushReducer();

		try {
			reducer.configure(job);
			fail();
		} catch (IllegalArgumentException e) {
			if (!"No input regex: crush.1.regex".equals(e.getMessage())) {
				throw e;
			}
		}
	}

	@Test
	public void missingOutputRegex() {
		JobConf job = new JobConf(false);

		job.set("mapred.tip.id", "task_201011081200_14527_r_1234");

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.set("mapred.output.dir", outDir.getAbsolutePath());

		job.setLong("crush.timestamp", 98765);

		job.setLong("dfs.block.size", 1024 * 1024 * 64L);

		job.setInt("crush.num.specs", 2);
		job.set("crush.0.regex", "foo");
		job.set("crush.0.regex.replacement", "bar");
		job.set("crush.0.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.0.output.format", TextOutputFormat.class.getName());

		job.set("crush.1.regex", "hello");
		job.set("crush.1.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.1.output.format", TextOutputFormat.class.getName());

		reducer = new CrushReducer();

		try {
			reducer.configure(job);
			fail();
		} catch (IllegalArgumentException e) {
			if (!"No output replacement: crush.1.regex.replacement".equals(e.getMessage())) {
				throw e;
			}
		}
	}

	@Test
	public void missingInputFormat() {
		JobConf job = new JobConf(false);

		job.set("mapred.tip.id", "task_201011081200_14527_r_1234");

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.set("mapred.output.dir", outDir.getAbsolutePath());

		job.setLong("crush.timestamp", 98765);

		job.setLong("dfs.block.size", 1024 * 1024 * 64L);

		job.setInt("crush.num.specs", 2);
		job.set("crush.0.regex", "foo");
		job.set("crush.0.regex.replacement", "bar");
		job.set("crush.0.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.0.output.format", SequenceFileOutputFormat.class.getName());

		job.set("crush.1.regex", "hello");
		job.set("crush.1.regex.replacement", "hello");
		job.set("crush.1.output.format", SequenceFileOutputFormat.class.getName());

		reducer = new CrushReducer();

		try {
			reducer.configure(job);
			fail();
		} catch (IllegalArgumentException e) {
			if (!"No input format: crush.1.input.format".equals(e.getMessage())) {
				throw e;
			}
		}
	}

	@Test
	public void inputFormatWrongType() {
		JobConf job = new JobConf(false);

		job.set("mapred.tip.id", "task_201011081200_14527_r_1234");

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.set("mapred.output.dir", outDir.getAbsolutePath());

		job.setLong("crush.timestamp", 98765);

		job.setLong("dfs.block.size", 1024 * 1024 * 64L);

		job.setInt("crush.num.specs", 2);
		job.set("crush.0.regex", "foo");
		job.set("crush.0.regex.replacement", "bar");
		job.set("crush.0.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.0.output.format", SequenceFileOutputFormat.class.getName());

		job.set("crush.1.regex", "hello");
		job.set("crush.1.regex.replacement", "hello");
		job.set("crush.1.input.format", Object.class.getName());
		job.set("crush.1.output.format", SequenceFileOutputFormat.class.getName());

		reducer = new CrushReducer();

		try {
			reducer.configure(job);
			fail();
		} catch (IllegalArgumentException e) {
			if (!"Not a file input format: crush.1.input.format=java.lang.Object".equals(e.getMessage())) {
				throw e;
			}
		}
	}

	@Test
	public void missingOutputFormat() {
		JobConf job = new JobConf(false);

		job.set("mapred.tip.id", "task_201011081200_14527_r_1234");

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.set("mapred.output.dir", outDir.getAbsolutePath());

		job.setLong("crush.timestamp", 98765);

		job.setLong("dfs.block.size", 1024 * 1024 * 64L);

		job.setInt("crush.num.specs", 2);
		job.set("crush.0.regex", "foo");
		job.set("crush.0.regex.replacement", "bar");
		job.set("crush.0.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.0.output.format", SequenceFileOutputFormat.class.getName());

		job.set("crush.1.regex", "hello");
		job.set("crush.1.regex.replacement", "hello");
		job.set("crush.1.input.format", SequenceFileInputFormat.class.getName());

		reducer = new CrushReducer();

		try {
			reducer.configure(job);
			fail();
		} catch (IllegalArgumentException e) {
			if (!"No output format: crush.1.output.format".equals(e.getMessage())) {
				throw e;
			}
		}
	}

	@Test
	public void outputFormatWrongType() {
		JobConf job = new JobConf(false);

		job.set("mapred.tip.id", "task_201011081200_14527_r_1234");

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.set("mapred.output.dir", outDir.getAbsolutePath());

		job.setLong("crush.timestamp", 98765);

		job.setLong("dfs.block.size", 1024 * 1024 * 64L);

		job.setInt("crush.num.specs", 2);
		job.set("crush.0.regex", "foo");
		job.set("crush.0.regex.replacement", "bar");
		job.set("crush.0.input.format", SequenceFileInputFormat.class.getName());
		job.set("crush.0.output.format", SequenceFileOutputFormat.class.getName());

		job.set("crush.1.regex", "hello");
		job.set("crush.1.regex.replacement", "hello");
		job.set("crush.1.input.format", TextInputFormat.class.getName());
		job.set("crush.1.output.format", Object.class.getName());

		reducer = new CrushReducer();

		try {
			reducer.configure(job);
			fail();
		} catch (IllegalArgumentException e) {
			if (!"Not an output format: crush.1.output.format=java.lang.Object".equals(e.getMessage())) {
				throw e;
			}
		}
	}
}

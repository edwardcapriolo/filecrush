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
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.m6d.filecrush.crush.Bucketer;
import com.m6d.filecrush.crush.FileStatusHasSize;
import com.m6d.filecrush.crush.Bucketer.Bucket;


/**
 * Block size 50 and threshold 75%.
 */
@RunWith(Parameterized.class)
public class BucketerParameterizedTest {
	@Parameters
	public static Collection<Object[]> testCases() {
		List<Object[]> testCases = new ArrayList<Object[]>();

		String dir;
		List<FileStatus> input;
		List<Bucket> expected;

		/*
		 * Three buckets of two each.
		 *
		 * 0					1						2
		 * file3 37		file 2 20		file 4 19
		 * file6 10		file 5 17		file 1 18
		 */
		dir = "three buckets of two each";

		input = asList(	statusFor("file1", 18),
										statusFor("file2", 20),
										statusFor("file3", 37),
										statusFor("file4", 19),
										statusFor("file5", 17),
										statusFor("file6", 10));

		expected = asList(new Bucket("three buckets of two each-0", asList("file3", "file6"), 47),
											new Bucket("three buckets of two each-1", asList("file2", "file5"), 37),
											new Bucket("three buckets of two each-2", asList("file4", "file1"), 37));

		testCases.add(new Object[] { dir, true, input, expected });


		/*
		 * Not enough data to fill all the buckets. Data should be packed into as few buckets as possible.
		 */
		dir = "not/enough/data/for/max/buckets";

		input = asList(	statusFor("file1", 1),
										statusFor("file2", 2),
										statusFor("file3", 3),
										statusFor("file4", 4),
										statusFor("file5", 5),
										statusFor("file6", 6));

		expected = asList(new Bucket("not/enough/data/for/max/buckets-0", asList("file6", "file5", "file4", "file3", "file2", "file1"), 21));

		testCases.add(new Object[] { dir, true, input, expected });

		/*
		 * A directory with one file should be ignored.
		 */
		dir = "dir/with/one/file";

		input = asList(statusFor("loner", 1));

		expected = emptyList();

		testCases.add(new Object[] { dir, true, input, expected });


		/*
		 * Test case with enough data to fill up all the buckets but no one bucket is more than twice the bucket size.
		 *
		 * 0						1						2						3						4
		 * file 9 35		file 1 30		file 3 30		file 5 30		file 7 30
		 * file 6 20		file 8 25		file 0 20		file 2 20		file 4 20
		 * 													file 11 20	file 10 10
		 */
		dir = "enough/data/for/max/buckets";

		input = asList(	statusFor("file0",	20),
										statusFor("file1",	30),
										statusFor("file2",	20),
										statusFor("file3",	30),
										statusFor("file4",	20),
										statusFor("file5",	30),
										statusFor("file6",	20),
										statusFor("file7",	30),
										statusFor("file8",	25),
										statusFor("file9",	35),
										statusFor("file10",	10),
										statusFor("file11",	20));

		expected = asList(
			new Bucket("enough/data/for/max/buckets-0", asList("file9", "file6"), 55),
			new Bucket("enough/data/for/max/buckets-1", asList("file1", "file8"), 55),
			new Bucket("enough/data/for/max/buckets-2", asList("file3", "file0", "file11"), 70),
			new Bucket("enough/data/for/max/buckets-3", asList("file5", "file2", "file10"), 60),
			new Bucket("enough/data/for/max/buckets-4", asList("file7", "file4"), 50));

		testCases.add(new Object[] { dir, true, input, expected });


		/*
		 * Test case with enough data to fill up all the buckets with some of the buckets more than twice the bucket size.
		 *
		 * 1						2						3						4						5
		 * file  0 35		file  2 35	file  4 35	file  6 35	file  8 35
		 * file 10 35		file 12 35	file 14 35	file  1 30	file  3 30
		 * file  9 30		file 11 30	file 13 30 	file  5 30	file  7 30
		 * 																			file 15 30	file 16 20
		 */
		dir = "enough/data/for/max/buckets/and/big/buckets";

		input = asList(	statusFor("file0",	35),
										statusFor("file1",	30),
										statusFor("file2",	35),
										statusFor("file3",	30),
										statusFor("file4",	35),
										statusFor("file5",	30),
										statusFor("file6",	35),
										statusFor("file7",	30),
										statusFor("file8",	35),
										statusFor("file9",	30),
										statusFor("file10",	35),
										statusFor("file11",	30),
										statusFor("file12",	35),
										statusFor("file13",	30),
										statusFor("file14",	35),
										statusFor("file15",	30),
										statusFor("file16",	20));

		expected = asList(
			new Bucket("enough/data/for/max/buckets/and/big/buckets-0", asList("file0", "file10", "file9"), 100),
			new Bucket("enough/data/for/max/buckets/and/big/buckets-1", asList("file2", "file12", "file11"), 100),
			new Bucket("enough/data/for/max/buckets/and/big/buckets-2", asList("file4", "file14", "file13"), 100),
			new Bucket("enough/data/for/max/buckets/and/big/buckets-3", asList("file6", "file1", "file5", "file15"), 125),
			new Bucket("enough/data/for/max/buckets/and/big/buckets-4", asList("file8", "file3", "file7", "file16"), 115));

		testCases.add(new Object[] { dir, true, input, expected });


		/*
		 * Exactly enough data for five buckets of 50.
		 */
		dir = "exactly/enough/data/for/max/buckets";

		input = asList(	statusFor("file0", 20),
										statusFor("file1", 30),
										statusFor("file2", 20),
										statusFor("file3", 30),
										statusFor("file4", 20),
										statusFor("file5", 30),
										statusFor("file6", 20),
										statusFor("file7", 30),
										statusFor("file8", 20),
										statusFor("file9", 30));

		expected = asList(
			new Bucket("exactly/enough/data/for/max/buckets-0", asList("file1", "file0"), 50),
			new Bucket("exactly/enough/data/for/max/buckets-1", asList("file3", "file2"), 50),
			new Bucket("exactly/enough/data/for/max/buckets-2", asList("file5", "file4"), 50),
			new Bucket("exactly/enough/data/for/max/buckets-3", asList("file7", "file6"), 50),
			new Bucket("exactly/enough/data/for/max/buckets-4", asList("file9", "file8"), 50));

		testCases.add(new Object[] { dir, true, input, expected });


		/*
		 * Exactly enough data for four buckets of 50.
		 */
		dir = "exactly/enough/data/for/four/buckets";

		input = asList(	statusFor("file0", 20),
										statusFor("file1", 30),
										statusFor("file2", 20),
										statusFor("file3", 30),
										statusFor("file4", 20),
										statusFor("file5", 30),
										statusFor("file6", 20),
										statusFor("file7", 30));

		expected = asList(
			new Bucket("exactly/enough/data/for/four/buckets-0", asList("file1", "file0"), 50),
			new Bucket("exactly/enough/data/for/four/buckets-1", asList("file3", "file2"), 50),
			new Bucket("exactly/enough/data/for/four/buckets-2", asList("file5", "file4"), 50),
			new Bucket("exactly/enough/data/for/four/buckets-3", asList("file7", "file6"), 50));

		testCases.add(new Object[] { dir, true, input, expected });


		/*
		 * Buckets that end up with one file are ignored.
		 *
		 * 0					1
		 * file 3 35	file 2 30
		 * 						file 1 25
		 *
		 * What would have been bucket 0 is dropped since it has only one file in it.
		 */
		dir = "buckets/with/one/file/are/ignored";

		input = asList(	statusFor("file1", 25),
										statusFor("file2", 30),
										statusFor("file3", 35));

		expected = asList(new Bucket("buckets/with/one/file/are/ignored-1", asList("file2", "file1"), 55));

		testCases.add(new Object[] { dir, true, input, expected });


		/*
		 * Set the flag so that single item buckets are returned.
		 *
		 * 0					1
		 * file 3 35	file 2 30
		 * 						file 1 25
		 *
		 * What would have been bucket 0 is dropped since it has only one file in it.
		 */
		dir = "include/buckets/with/one/file";

		input = asList(	statusFor("file1", 25),
										statusFor("file2", 30),
										statusFor("file3", 35));

		expected = asList(
				new Bucket("include/buckets/with/one/file-0", asList("file3"), 35),
				new Bucket("include/buckets/with/one/file-1", asList("file2", "file1"), 55));

		testCases.add(new Object[] { dir, false, input, expected });

		return testCases;
	}

	private final Bucketer bucketer;

	private final String dir;

	private final List<FileStatus> input;

	private final List<Bucket> expected;

	public BucketerParameterizedTest(String dir, boolean excludeSingleItemBuckets, List<FileStatus> input, List<Bucket> expected) {
		super();

		this.dir = dir;
		this.input = input;
		this.expected = expected;

		this.bucketer = new Bucketer(5, 50, excludeSingleItemBuckets);
	}

	@Test
	public void test() {
		bucketer.reset(dir);

		for (int i = 0; i < input.size(); i++) {
			FileStatus file = input.get(i);

			bucketer.add(new FileStatusHasSize(file));

			assertThat(dir, bucketer.count(), equalTo(i + 1));
		}

		List<Bucket> actual = bucketer.createBuckets();

		Collections.sort(expected, BUCKET_CMP);
		Collections.sort(actual, BUCKET_CMP);

		assertThat(dir, actual, equalTo(expected));

		assertThat(dir, bucketer.count(), equalTo(0));
		assertThat(dir, bucketer.dir(), nullValue());
		assertThat(dir, bucketer.size(), equalTo(0L));
	}

	private static FileStatus statusFor(String path, long size) {
		return new FileStatus(size, false, 3, 1024, currentTimeMillis(), new Path(path));
	}

	private static final Comparator<Bucket> BUCKET_CMP = new Comparator<Bucket>() {
		@Override
		public int compare(Bucket o1, Bucket o2) {
			return o1.name().compareTo(o2.name());
		}
	};
}

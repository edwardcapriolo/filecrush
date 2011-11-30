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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Before;
import org.junit.Test;

import com.m6d.filecrush.crush.KeyValuePreservingTextInputFormat.KeyValuePreservingRecordReader;


public class KeyValuePreservingRecordReaderNextTest implements RecordReader<LongWritable, Text> {

	private final Text key = new Text();

	private final Text value = new Text();

	private boolean next;

	private long offset;

	private String line;

	private KeyValuePreservingRecordReader reader;

	@Before
	public void before() {
		reader = new KeyValuePreservingRecordReader(this);
	}

	@Test
	public void nextDelegation() throws IOException {
		next = false;

		assertThat(reader.next(key, value), is(false));
	}

	@Test
	public void keyAndValueArePreserved() throws IOException {
		next = true;

		/*
		 * Key with multiple values.
		 */
		offset = 0;
		line = "key\tvalue0\tvalue1\tvalue2";

		assertThat(reader.next(key, value), is(true));

		assertThat(key.toString(), equalTo("key"));
		assertThat(value.toString(), equalTo("value0\tvalue1\tvalue2"));


		/*
		 * No key with tab and value.
		 */
		offset = offset + line.length() + 1;
		line = "\tvalue0\tvalue1\tvalue2";
		assertThat(reader.next(key, value), is(true));

		assertThat(key.toString(), equalTo(""));
		assertThat(value.toString(), equalTo("value0\tvalue1\tvalue2"));


		/*
		 * Key and tab, no value.
		 */
		offset = offset + line.length() + 1;
		line = "key and tab\t";
		assertThat(reader.next(key, value), is(true));

		assertThat(key.toString(), equalTo("key and tab"));
		assertThat(value.toString(), equalTo(""));


		/*
		 * Key only. No tab or value.
		 */
		offset = offset + line.length() + 1;
		line = "key only";
		assertThat(reader.next(key, value), is(true));

		assertThat(key.toString(), equalTo("key only"));
		assertThat(value.toString(), equalTo(""));


		/*
		 * Key and value again.
		 */
		offset = offset + line.length() + 1;
		line = "a reeeeeeeally long key\tvalue0\tvalue1\tvalue2\tvalue3\tvalue4";
		assertThat(reader.next(key, value), is(true));

		assertThat(key.toString(), equalTo("a reeeeeeeally long key"));
		assertThat(value.toString(), equalTo("value0\tvalue1\tvalue2\tvalue3\tvalue4"));
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		if (next) {
			key.set(offset);
			value.set(line);
		}

		return next;
	}

	@Override
	public LongWritable createKey() {
		throw new AssertionError();
	}

	@Override
	public Text createValue() {
		throw new AssertionError();
	}

	@Override
	public long getPos() throws IOException {
		throw new AssertionError();
	}

	@Override
	public void close() throws IOException {
		throw new AssertionError();
	}

	@Override
	public float getProgress() throws IOException {
		throw new AssertionError();
	}
}

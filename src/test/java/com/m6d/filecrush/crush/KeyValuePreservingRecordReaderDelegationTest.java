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

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.m6d.filecrush.crush.KeyValuePreservingTextInputFormat.KeyValuePreservingRecordReader;


@RunWith(MockitoJUnitRunner.class)
public class KeyValuePreservingRecordReaderDelegationTest {

	@Mock
	private PartialRecordReader delegate;

	private KeyValuePreservingRecordReader reader;

	@Before
	public void before() {
		reader = new KeyValuePreservingRecordReader(delegate);
	}

	@Test
	public void createValueDelegation() {
		reader.createValue();

		verify(delegate).createValue();
	}

	@Test
	public void getPosDelegation() throws IOException {
		reader.getPos();

		verify(delegate).getPos();
	}

	@Test
	public void closeDelegation() throws IOException {
		reader.close();

		verify(delegate).close();
	}

	public void createKeyDoesNotDelegate() {
		Text key = reader.createKey();

		assertThat(key, not(nullValue()));
		assertThat(reader.createKey(), not(sameInstance(key)));
	}

	public static abstract class PartialRecordReader implements RecordReader<LongWritable, Text> {
		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			throw new AssertionError();
		}

		@Override
		public LongWritable createKey() {
			throw new AssertionError();
		}
	}
}

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

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.FileStatus;
import org.junit.Before;
import org.junit.Test;

import com.m6d.filecrush.crush.Bucketer;
import com.m6d.filecrush.crush.FileStatusHasSize;
import com.m6d.filecrush.crush.Bucketer.HasSize;


public class BucketerTest {

	private Bucketer bucketer;

	@Before
	public void before() {
		bucketer = new Bucketer(5, 50, true);
	}

	@Test(expected = IllegalStateException.class)
	public void callAddBeforeReset() {
		bucketer.add(new FileStatusHasSize(new FileStatus()));
	}

	@Test(expected = IllegalStateException.class)
	public void callCreateBeforeReset() {
		bucketer.createBuckets();
	}

	@Test
	public void addNullCheck() {
		bucketer.reset("foo");

		try {
			bucketer.add(null);
			fail();
		} catch (NullPointerException ok) {
		}
	}

	@Test(expected = NullPointerException.class)
	public void resestNullCheck() {
		bucketer.reset(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void resestEmptyCheck() {
		bucketer.reset("");
	}

	@Test
	public void nothingAdded() {
		bucketer.reset("test");

		assertThat(bucketer.createBuckets(), equalTo((Object) emptyList()));
	}

	@Test
	public void addZeroSize() {
		bucketer.reset("test");

		bucketer.add(new HasSize() {
			@Override
			public String id() {
				return "test";
			}

			@Override
			public long size() {
				return 0;
			}
		});

		assertThat(bucketer.createBuckets(), equalTo((Object) emptyList()));
	}
}


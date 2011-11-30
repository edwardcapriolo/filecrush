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

import java.io.IOException;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import com.m6d.filecrush.crush.CountersMapper;
import com.m6d.filecrush.crush.MapperCounter;

@SuppressWarnings("deprecation")
public class CountersMapperTest extends EasyMockSupport {

	private Reporter reporter;

	private CountersMapper mapper;

	@Before
	public void before() {
		reporter = createMock("reporter", Reporter.class);

		mapper = new CountersMapper();
	}

	@Test
	public void map() throws IOException {
		Counters counters = new Counters();

		counters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.DIRS_FOUND.name(), 1);

		counters.incrCounter(MapperCounter.DIRS_ELIGIBLE, 2);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.DIRS_ELIGIBLE.name(), 2);

		counters.incrCounter(MapperCounter.DIRS_SKIPPED, 3);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.DIRS_SKIPPED.name(), 3);

		counters.incrCounter(MapperCounter.FILES_FOUND, 4);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.FILES_FOUND.name(), 4);

		counters.incrCounter(MapperCounter.FILES_SKIPPED, 5);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.FILES_SKIPPED.name(), 5);

		replayAll();

		mapper.map(counters, null, null, reporter);

		verifyAll();
	}
}

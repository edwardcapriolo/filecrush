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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Exists only to load the counters created during the planning phase into the reporter.
 */
@SuppressWarnings("deprecation")
public class CountersMapper implements Mapper<Counters, NullWritable, Text, Text> {

	@Override
	public void configure(JobConf job) {
		/*
		 * Nothing to do here.
		 */
	}

	@Override
	public void map(Counters key, NullWritable value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
		for (Group group : key) {
			for (Counter counter : group) {
				reporter.incrCounter(group.getName(), counter.getName(), counter.getValue());
			}
		}
	}

	@Override
	public void close() throws IOException {
		/*
		 * Nothing to do here.
		 */
	}
}

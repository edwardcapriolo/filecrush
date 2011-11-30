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

import org.apache.hadoop.fs.FileStatus;

import com.m6d.filecrush.crush.Bucketer.HasSize;


class FileStatusHasSize implements HasSize {

	private final FileStatus fileStatus;

	public FileStatusHasSize(FileStatus fileStatus) {
		super();

		if (null == fileStatus) {
			throw new NullPointerException("File status");
		}

		this.fileStatus = fileStatus;
	}

	@Override
	public String id() {
		return fileStatus.getPath().toUri().getPath();
	}

	@Override
	public long size() {
		return fileStatus.getLen();
	}
}

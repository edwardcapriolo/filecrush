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
package com.m6d.filecrush.clean;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.m6d.filecrush.clean.Clean;

public class TestClean  extends HadoopTestCase{

	private static final Path ROOT_DIR = new Path("testing");

	public TestClean() throws IOException {
		super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
	}

	private Path getDir(Path dir) {
		if (isLocalFS()) {
			String localPathRoot = System
			.getProperty("test.build.data", "/tmp").replace(' ', '+');
			dir = new Path(localPathRoot, dir);
		}
		return dir;
	}

	public void setUp() throws Exception {
		super.setUp();
		Path rootDir = getDir(ROOT_DIR);
		Configuration conf = createJobConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(rootDir, true);		
	}
	
	@Test
	public void testAge() throws Exception{
		Configuration conf = createJobConf();
		FileSystem fs = FileSystem.get(conf);
		fs.mkdirs( new Path(ROOT_DIR,"a") );
		fs.mkdirs( new Path( new Path(ROOT_DIR,"a"),"1") );
		fs.mkdirs( new Path(ROOT_DIR,"b") );
		fs.mkdirs( new Path(ROOT_DIR,"c") );
		fs.mkdirs( new Path( new Path(ROOT_DIR,"c"),"2") );
		
		Path oldFile = new Path(new Path( new Path(ROOT_DIR,"a"),"1"),"oldfile");
		FSDataOutputStream out = fs.create(oldFile);
		out.write("bla".getBytes());
		out.close();
		
		Path cFile = new Path(new Path( new Path(ROOT_DIR,"c"),"1"),"cfile");
		FSDataOutputStream out2 = fs.create(cFile);
		out2.write("wah".getBytes());
		out2.close();
		
		assertEquals(true,fs.exists(cFile));
		assertEquals(true,fs.exists(oldFile));
		
		Clean cleanWarn = new Clean();
		Configuration warnConf = createJobConf();
		warnConf.set(Clean.TARGET_DIR, ROOT_DIR.toString());
		warnConf.set(Clean.TARGET_EXPR, "cfile");
		warnConf.set(Clean.WARN_MODE, "true");
		ToolRunner.run(warnConf, cleanWarn, new String[]{});
		assertEquals(true,fs.exists(cFile));
		assertEquals(true,fs.exists(oldFile));
				
		Clean cleanReg = new Clean();
		Configuration regConf = createJobConf();
		regConf.set(Clean.TARGET_DIR, ROOT_DIR.toString());
		regConf.set(Clean.TARGET_EXPR, "cfile");
		ToolRunner.run(regConf, cleanReg, new String[]{});
		assertEquals(false,fs.exists(cFile));
		assertEquals(true,fs.exists(oldFile));
		
		Clean clean = new Clean();
		Configuration cleanConf = createJobConf();
		cleanConf.setLong(Clean.CUTTOFF_MILLIS, 20000);
		cleanConf.set(Clean.TARGET_DIR, ROOT_DIR.toString());
		ToolRunner.run(cleanConf, clean, new String[]{});
		assertEquals(true,fs.exists(oldFile));
		Thread.sleep(3);
		
		Clean clean2 = new Clean();
		Configuration cleanConf2 = createJobConf();
		cleanConf2.setLong(Clean.CUTTOFF_MILLIS, 1);
		cleanConf2.set(Clean.TARGET_DIR, ROOT_DIR.toString());
		ToolRunner.run(cleanConf2, clean2, new String[]{});
		assertEquals(false,fs.exists(oldFile));
		
	}
	
	@Test
	public void testNegatives() throws Exception{
		Clean clean = new Clean();
		Configuration cleanConf = createJobConf();
		cleanConf.setLong(Clean.CUTTOFF_MILLIS, 20000);
		cleanConf.set(Clean.TARGET_DIR, ROOT_DIR.toString());
		cleanConf.set(Clean.TARGET_EXPR, "bla");
		int res = ToolRunner.run(cleanConf, clean, new String[]{});
		assertEquals(9,res);
	}

	@Test
	public void testRootClean() throws Exception{
		Clean clean = new Clean();
		Configuration cleanConf = createJobConf();
		cleanConf.set(Clean.TARGET_DIR, "/");
		cleanConf.set(Clean.TARGET_EXPR, "bla");
		int res = ToolRunner.run(cleanConf, clean, new String[]{});
		assertEquals(2,res);
	}
}
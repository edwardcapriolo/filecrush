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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class Clean extends Configured implements Tool{

	public static final String TARGET_DIR="clean.target.dir";
	public static final String CUTTOFF_MILLIS="clean.cutoff.millis";
	public static final String TARGET_EXPR="clean.target.expr";
	public static final String WARN_MODE="clean.warn.mode";
	
	protected FileSystem fs;
	protected Configuration conf;
	protected long cutoff;
	
	public Clean(){
		super();
	}
	
	public static void main(String[] args) throws Exception {
		Clean clean = new Clean();
		int exitCode = ToolRunner.run(new Configuration(),clean, args);
		System.exit(exitCode);
    }
	
	@Override
	public int run(String[] args) throws Exception {
        conf = getConf();
       
        Path targetDir = new Path(conf.get(TARGET_DIR));

		try {
			fs = targetDir.getFileSystem(conf);
		} catch (IOException e) {
			throw new RuntimeException("Could not open filesystem");
		}
		int pre = preFlightCheck();
		if (pre!=0){
			return pre;
		}
		
		if (conf.get(CUTTOFF_MILLIS)!=null){
			long now=System.currentTimeMillis();
			long targetAge= Long.parseLong(conf.get(CUTTOFF_MILLIS));
			cutoff=now-targetAge;
		}
		
        return cleanup(targetDir);
    
	}
	
	public void warnOrDelete(Path p) throws IOException{
		if (conf.getBoolean(WARN_MODE, false)){
			System.out.println("DELETE "+p);
		} else {
			if ( p.equals( new Path(conf.get(TARGET_DIR)) )){
				
			} else {
				fs.delete(p);
			}
		}
	}
	
	
	public int cleanup(Path p){
		try {
			if (fs.isFile(p)){
				if (conf.get(TARGET_EXPR)!=null){
					if (p.getName().matches(conf.get(TARGET_EXPR))){
						warnOrDelete(p);
					}
				}
				if (conf.get(CUTTOFF_MILLIS)!=null){
					if (fs.getFileStatus(p).getModificationTime() < cutoff ){
						warnOrDelete(p);
					} 
				}
			}
			
			if (fs.isDirectory(p)){
				for (FileStatus stat: fs.listStatus(p)){
					cleanup( stat.getPath() );
				}
				if (fs.listStatus(p).length == 0){
					if (conf.get(TARGET_EXPR)!=null){
						if (p.getName().matches(conf.get(TARGET_EXPR))){
							warnOrDelete(p);
						}
					}
					if (conf.get(CUTTOFF_MILLIS)!=null){
						if (fs.getFileStatus(p).getModificationTime() < cutoff ){
							warnOrDelete(p);
						}
					}
				}
			}
		} catch (IOException e) {
			System.out.println("exception "+e);
			return 7;
		}
		return 0;
	}
	
	public int preFlightCheck(){
		Configuration conf = getConf();
		if (conf.get(TARGET_DIR) == null){
        	System.err.println("You must specify a target.dir");
        	return 1;
        }
        if (conf.get(TARGET_DIR).equals("/")){
        	System.err.println("Will not clean / !!!!!!");
        	return 2;
        }
        if ( fs.getHomeDirectory().equals( new Path(conf.get(TARGET_DIR)) ) ){
        	System.err.println("Will not clean home directory");
        	return 3;
        }
        if (conf.get(CUTTOFF_MILLIS)==null && conf.get(TARGET_EXPR)==null){
        	System.err.println("You must specify "+CUTTOFF_MILLIS+" or "+TARGET_EXPR);
        	return 4;
        }
        if (!(conf.get(CUTTOFF_MILLIS)==null) && !(conf.get(TARGET_EXPR)==null)){
        	System.err.println("You can not specify "+CUTTOFF_MILLIS+" and "+TARGET_EXPR);
        	return 9;
        }
        if (conf.get(CUTTOFF_MILLIS)!=null) {
        	try { 
        		Long.parseLong(conf.get(CUTTOFF_MILLIS));
        	} catch (NumberFormatException ex){
        		System.err.println(CUTTOFF_MILLIS+" was specified as "+conf.get(CUTTOFF_MILLIS)+" this is not a long integer");
            	return 15;
        	}
        }
        try {
			if (! fs.exists( new Path(conf.get(TARGET_DIR)))) {
				System.err.println(conf.get(TARGET_DIR)+" does not exist");
			}
		} catch (IOException e) {
			System.err.println("IOEXCEPTION"+ e);
			return 6;
		}
        return 0;
	}
	
}
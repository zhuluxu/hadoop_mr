package com.hgx.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;

public class HdfsUtil {

	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream is = fs.open(new Path("/jdk-7u65-linux-i586.tar.gz"));
		FileOutputStream os = new FileOutputStream("c:/jdk.tgz");
		IOUtils.copy(is, os);
	}
	
}

package com.hgx.hadoop.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountRunner {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		conf.set("mapreduce.job.jar", "mywc.jar");

		Job job = Job.getInstance(conf);
		

		job.setJarByClass(WordCountRunner.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://weekend-1206-01:9000/wc/data"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://weekend-1206-01:9000/wc/output3"));

		job.waitForCompletion(true);
		
		
	}
}

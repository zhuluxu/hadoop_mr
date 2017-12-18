package com.hgx.hadoop.mapper.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WorkCountRunner {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WorkCountRunner <input path> <output path>");
            System.exit(-1);
        }

        /**构造一个配置对象，读取配置文件，或者往该对象中设值*/
        Configuration config = new Configuration();

        /**创建job对象，用来描述本任务的相关信息*/
        Job job = Job.getInstance(config);

        /**本job所用的jar包就是本类所在的jar包*/
        job.setJarByClass(WorkCountRunner.class);

        job.setJarByClass(WorkCountRunner.class);
        /**本job使用哪些类用来作为mapper和reducer*/
        job.setMapperClass(WorkCountMapper.class);
        job.setReducerClass(WorkCountReducer.class);

        /**本job中mapper的输出数据key的类型*/
        job.setMapOutputKeyClass(Text.class);
        /**本job中mapperd的输出数据vlaue的类型*/
        job.setMapOutputValueClass(LongWritable.class);

        /**本job中reducer的输出数据key的类型*/
        job.setOutputKeyClass(Text.class);
        /**本job中reducer的输出数据value的类型*/
        job.setOutputValueClass(LongWritable.class);

        /**指定本job所输入路径*/
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        /**指定本job所输出路径*/
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /**将任务提交给集群*/

        System.exit(job.waitForCompletion(true)?1:0);
    }
}

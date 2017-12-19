package com.hgx.hadoop.combiner;

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

        Configuration config = new Configuration();

        Job job = Job.getInstance(config);

        job.setJarByClass(WorkCountRunner.class);

        job.setMapperClass(WorkCountMapper.class);
        job.setReducerClass(WorkCountReducer.class);

        /**指定本地的combiner类*/
        job.setCombinerClass(WorkCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /**将任务提交给集群*/
        System.exit(job.waitForCompletion(true)?1:0);
    }
}

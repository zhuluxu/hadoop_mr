package com.hgx.hadoop.InverteIndex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: hegx
 * @Description:
 * @Date: 15:02 2017/12/20
 */
public class InvertIndexSort {

    private static class InvertIndexSortMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k = new Text();

        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = StringUtils.split(line, "\t");

            String word = fields[0];//名字

            String fileNameAndAmount = fields[1];//文件名跟数量

            k.set(word);
            v.set(fileNameAndAmount);
            context.write(k, v);
        }

        private static class InvertIndexSortReducer extends Reducer<Text, Text, Text, NullWritable> {

            private Text k = new Text();

            private NullWritable v = NullWritable.get();

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                StringBuffer buffer = new StringBuffer(key.toString()).append("-->");

                for (Text value : values) {
                    buffer.append(value).append(",");
                }

                String result = StringUtils.substring(buffer.toString(), 0, buffer.toString().length() - 1);

                k.set(result);
                context.write(k, v);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();

        Job job = Job.getInstance(config);

        job.setJarByClass(InvertIndexSort.class);
        job.setMapperClass(InvertIndexSortMapper.class);
        job.setReducerClass(InvertIndexSortMapper.InvertIndexSortReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);
    }

}

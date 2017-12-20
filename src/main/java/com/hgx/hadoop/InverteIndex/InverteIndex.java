package com.hgx.hadoop.InverteIndex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: hegx
 * @Description:
 * @Date: 13:31 2017/12/20
 */
public class InverteIndex {

    private static class InvertIndexMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Text k = new Text();

        private LongWritable v = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = StringUtils.split(line, " ");

            FileSplit inputSplit = (FileSplit) context.getInputSplit();//InputSplit 这里使用的是它的实现类

            String fileName = inputSplit.getPath().getName();//获取文件名来源

            for (String word : fields) {
                k.set(word + "-->" + fileName);
                v.set(1);
                context.write(k, v);
            }
        }
    }


    private static class InvertIndexReducer extends Reducer<Text, LongWritable, Text, Text> {

        private Text k = new Text();

        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            String[] keyMsg = StringUtils.split(key.toString(), "-->");

            String word = keyMsg[0];

            String fileName = keyMsg[1];

            long count = 0;

            for (LongWritable value : values) {
                count += value.get();
            }
            k.set(word);
            v.set(fileName + " " + count);
            context.write(k, v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        Job job = Job.getInstance(config);

        job.setJarByClass(InverteIndex.class);
        job.setMapperClass(InvertIndexMapper.class);
        job.setReducerClass(InvertIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }

}

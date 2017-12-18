package com.hgx.hadoop.mapper.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WorkCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String works[] = line.split(" ");

        for (String work:works)
        {
            context.write(new Text(work),new LongWritable(1));
        }

    }
}

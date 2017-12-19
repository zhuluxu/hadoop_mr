package com.hgx.hadoop.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WorkCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        String work = key.toString();

        long count = 0;

        for (LongWritable value:values)
        {
            count+=value.get();
        }
        context.write(new Text(work),new LongWritable(count));
    }
}

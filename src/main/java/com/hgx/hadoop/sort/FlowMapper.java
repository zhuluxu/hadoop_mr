package com.hgx.hadoop.sort;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hegx
 * @Description:
 * @Date: 14:09 2017/12/19
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

    private static Log log = LogFactory.getLog(FlowMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = StringUtils.split(line, "\t");

        try {
            /**下行流量*/
            long flow_low = Long.valueOf(fields[1]);

            /**上行流量*/
            long flow_up = Long.valueOf(fields[2]);

            FlowBean flowBean = new FlowBean(fields[0], flow_low, flow_up);

            context.write(flowBean,NullWritable.get());

        } catch (NumberFormatException e) {

            log.debug("数组越界问题");

            e.printStackTrace();
        }


    }
}

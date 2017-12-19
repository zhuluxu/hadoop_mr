package com.hgx.hadoop.partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hegx
 * @Description:
 * @Date: 14:09 2017/12/19
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private static Log log = LogFactory.getLog(FlowMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /**拿到一行*/
        String line = value.toString();

        /**切分个字段*/
        String[] fiels = StringUtils.split(line, '\t');

        /**获取手机号码*/
        String phoneNumber = fiels[1];

        try {
            /**下行流量*/
            long flow_low = Long.valueOf(fiels[fiels.length - 3]);

            /**上行流量*/
            long flow_up = Long.valueOf(fiels[fiels.length - 2]);

            FlowBean flowBean = new FlowBean(phoneNumber, flow_low, flow_up);

            context.write(new Text(phoneNumber),flowBean);

        } catch (NumberFormatException e) {

            log.debug("数组越界问题");

            e.printStackTrace();
        }
    }
}

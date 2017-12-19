package com.hgx.hadoop.serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: hegx
 * @Description:
 * @Date: 14:27 2017/12/19
 */
public class FlowRuducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        /**获取手机号码*/
        String phoneNumber = key.toString();

        /**计算下行流量*/
        long sumFlowLow = 0;

        /**计算上行流量**/
        long sumFlowUp = 0;

        /**计算下行流量跟上行流量的综总和*/
        long sumFlowAmount = 0;

        if (values != null) {
            for (FlowBean flowBean : values) {
                sumFlowLow += flowBean.getFlowLow();
                sumFlowUp += flowBean.getFlowUp();
                sumFlowAmount += flowBean.getSumFlowAmount();
            }
        }
        context.write(key,new FlowBean(phoneNumber,sumFlowLow,sumFlowUp,sumFlowAmount));
    }
}

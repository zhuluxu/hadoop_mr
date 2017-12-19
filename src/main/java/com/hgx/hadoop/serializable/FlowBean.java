package com.hgx.hadoop.serializable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: hegx
 * @Description:用来输出的bean
 * @Date: 14:31 2017/12/19
 */
public class FlowBean  implements Writable {

    /**手机号码*/
    private String phoneNumber;

    /**下行流量*/
    private long flowLow;

    /**上行流量*/
    private long flowUp;

    /**流量总和*/
    private long sumFlowAmount;

    public FlowBean() {
    }

    public FlowBean(String phoneNumber, long flowLow, long flowUp) {
        this.phoneNumber = phoneNumber;
        this.flowLow = flowLow;
        this.flowUp = flowUp;
        this.sumFlowAmount = flowLow+flowUp;
    }

    public FlowBean(String phoneNumber, long flowLow, long flowUp, long sumFlowAmount) {
        this.phoneNumber = phoneNumber;
        this.flowLow = flowLow;
        this.flowUp = flowUp;
        this.sumFlowAmount = sumFlowAmount;
    }

    /**流式写出*/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNumber);
        dataOutput.writeLong(flowLow);
        dataOutput.writeLong(flowUp);
        dataOutput.writeLong(sumFlowAmount);
    }

    /***反序列化过程*/
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phoneNumber = dataInput.readUTF();
        flowLow = dataInput.readLong();
        flowUp = dataInput.readLong();
        sumFlowAmount = dataInput.readLong();
    }

    /********************getter,setter*********************************/
    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public long getFlowLow() {
        return flowLow;
    }

    public void setFlowLow(long flowLow) {
        this.flowLow = flowLow;
    }

    public long getFlowUp() {
        return flowUp;
    }

    public void setFlowUp(long flowUp) {
        this.flowUp = flowUp;
    }

    public long getSumFlowAmount() {
        return sumFlowAmount;
    }

    public void setSumFlowAmount(long sumFlowAmount) {
        this.sumFlowAmount = sumFlowAmount;
    }

    /**重写toString方法*/
    @Override
    public String toString() {
        return ""+phoneNumber+"\t"+this.flowLow+"\t"+this.flowUp+"\t"+this.sumFlowAmount;
    }
}

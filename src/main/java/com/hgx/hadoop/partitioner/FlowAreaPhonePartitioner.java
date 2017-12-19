package com.hgx.hadoop.partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class FlowAreaPhonePartitioner<KEY,VALUE> extends Partitioner<KEY,VALUE> {

    private static HashMap<String,Integer> map = new HashMap<String, Integer>();

    static {
        map.put("136",1);
        map.put("137",2);
        map.put("138",3);
        map.put("139",4);
    }

    @Override
    public int getPartition(KEY key, VALUE value, int i) {
       /* *//**电话号码的前三位*//*
        String pre_phoneValue =null;
        if (key!=null){
            pre_phoneValue  = StringUtils.substring(key.toString(), 0, 3);
        }
        return pre_phoneValue==null?0:map.get(pre_phoneValue);*/
        return map.get(key.toString().substring(0, 3))==null?4:map.get(key.toString().substring(0, 3));
    }
}

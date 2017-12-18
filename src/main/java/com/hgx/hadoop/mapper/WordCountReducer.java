package com.hgx.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {

		//�õ���һ�����ݵ�key������һ������
		String word = key.toString();
		//����һ���ۼӼ�����
		long count = 0;
		//�������key������һ��value�����Ǻܶ��1���������ۼ�
		for(LongWritable value:values){
			count += value.get();
		}
		
		//�����һ�����ʵ�ͳ�ƽ�����ѵ�����Ϊkey���ܴ�����Ϊvalue
		context.write(new Text(word), new LongWritable(count));
	}
	
	
}

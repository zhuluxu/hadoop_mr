package com.hgx.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// �õ���һ�е�����
		String line = value.toString();
		// �з���һ�еõ�һ�����ʵ�����
		String[] words = line.split(" ");
		// ��ÿ�����ʸ���һ�����������ȥ��������Ϊ�����key��������Ϊ�����value
		for (String word : words) {

			// ���������������<hello,1>������key-value��
			context.write(new Text(word), new LongWritable(1));
		}

	}

}

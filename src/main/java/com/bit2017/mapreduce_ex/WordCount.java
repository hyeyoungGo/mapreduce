package com.bit2017.mapreduce_ex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordCount");
		
		// 1. job instance 초기화 작업
		// job.setJarByClass(WordCount.class);
		
		// 실행
		job.waitForCompletion(true);
	}

}

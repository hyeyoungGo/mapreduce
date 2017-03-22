package com.bit2017.mapreduce.topn;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountCitation {

	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		private static LongWritable one = new LongWritable(1L);

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, one);
		}
	}	
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			for( LongWritable value : values ) {
				sum += value.get();
			}
		context.write( key, new LongWritable( sum ) );
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "CountCitation" );
		
		//1. job instance 초기화 작업
		job.setJarByClass( CountCitation.class );
		
		//2. mapper 클래스 지정
		job.setMapperClass( MyMapper.class );
		
		//3. reducer 클래스 지정
		job.setReducerClass( MyReducer.class );
		
		//4. 출력
		job.setMapOutputKeyClass( Text.class );
		
		//5. 출력 value 타입
		job.setMapOutputValueClass( LongWritable.class );
		
		//6. 입력 파일 포멧 지정(생략 가능)
		job.setInputFormatClass( TextInputFormat.class );
		
		//7. 출력 파일 포멧 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//8. 입력파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//9. 출력 디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 실행
		job.waitForCompletion( true );
	}

}

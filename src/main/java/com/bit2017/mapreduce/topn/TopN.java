package com.bit2017.mapreduce.topn;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopN {
	
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		private int topN = 10;
		private PriorityQueue<ItemFreq> pq = null;
		
		@Override
		protected void setup(Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			topN = context.getConfiguration().getInt( "topN", 10 );
			pq = new PriorityQueue<ItemFreq>( 10, new ItemFreqComparator() );
		}
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			ItemFreq newItemFreq = new ItemFreq();
			newItemFreq.setItem( key.toString() );
			newItemFreq.setFreq( Long.parseLong( value.toString() ) );
			
			ItemFreq head = pq.peek();	// 첫번째 item을 참조만함.
			if( head == null || head.getFreq() < newItemFreq.getFreq() ) {
				pq.add( newItemFreq );
			}
			
			if( pq.size() > topN ) {  // 첫번째 item을 끄집어 냄
				pq.remove();
			}
		}
			
		@Override
		protected void cleanup(Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			while( pq.isEmpty() == false ) {
				ItemFreq itemFreq = pq.remove();
				context.write( new Text( itemFreq.getItem() ), new LongWritable( itemFreq.getFreq() ) );
			}
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private int topN = 10;
		private PriorityQueue<ItemFreq> pq = null;
		
		@Override
		protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			topN = context.getConfiguration().getInt( "topN", 10 );
			pq = new PriorityQueue<ItemFreq>( 10, new ItemFreqComparator() );
		}
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
			Long sum = 0L;
			for( LongWritable value : values ) {
				sum += value.get();
			}
			
			ItemFreq newItemFreq = new ItemFreq();
			newItemFreq.setItem( key.toString() );
			newItemFreq.setFreq( sum );
			
			ItemFreq head = pq.peek();	// 첫번째 item을 참조만함.
			if( head == null || head.getFreq() < newItemFreq.getFreq() ) {
				pq.add( newItemFreq );
			}
			
			if( pq.size() > topN ) {  // 첫번째 item을 끄집어 냄
				pq.remove();
			}
		}
		
		@Override
		protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			while( pq.isEmpty() == false ) {
				ItemFreq itemFreq = pq.remove();
				context.write( new Text( itemFreq.getItem() ), new LongWritable( itemFreq.getFreq() ) );
			}
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "TopN" );
		
		//1. job instance 초기화 작업
		job.setJarByClass( TopN.class );
		
		//2. mapper 클래스 지정
		job.setMapperClass( MyMapper.class );
		
		//3. reducer 클래스 지정
		job.setReducerClass( MyReducer.class );
		
		//4. 출력
		job.setMapOutputKeyClass( Text.class );
		
		//5. 출력 value 타입
		job.setMapOutputValueClass( LongWritable.class );
		
		//6. 입력 파일 포멧 지정(생략 가능)
		job.setInputFormatClass( KeyValueTextInputFormat.class );
		
		//7. 출력 파일 포멧 지정(생략 가능)
		job.setOutputFormatClass( TextOutputFormat.class );
		
		//8. 입력파일 이름 지정
		FileInputFormat.addInputPath( job, new Path(args[0]) );
		
		//9. 출력 디렉토리 지정
		FileOutputFormat.setOutputPath( job, new Path(args[1]) );
		
		//10. N 파라미터
		job.getConfiguration().setInt( "topN", Integer.parseInt(args[2]) );
		
		//11. 실행
		job.waitForCompletion( true );
	}

}

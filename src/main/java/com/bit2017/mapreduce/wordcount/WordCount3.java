package com.bit2017.mapreduce.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.io.NumberWritable;
import com.bit2017.mapreduce.io.StringWritable;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

public class WordCount3 {
private static Log log = LogFactory.getLog(WordCount.class);
	
	public static class MyMapper extends Mapper<Text, Text, StringWritable, NumberWritable> {
		private StringWritable word = new StringWritable();
		private static NumberWritable one = new NumberWritable(1L);

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> MyMapper.map() called");
			String line = value.toString();
			StringTokenizer tokenize = new StringTokenizer(line, "\r\n\t,|()<> ''");
			while (tokenize.hasMoreTokens()) {
				word.set(tokenize.nextToken().toLowerCase());
				context.write(word, one);
			}
		}
	}	
	
	public static class MyReducer extends Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable> {
		
		private NumberWritable sumWritable = new NumberWritable();
		
		@Override
		protected void reduce(StringWritable key, Iterable<NumberWritable> values,
				Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			for( NumberWritable value : values ) {
				sum += value.get();
			}
			
		sumWritable.set( sum );
		context.write( key, sumWritable );
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "WordCount3" );
		
		//1. job instance 초기화 작업
		job.setJarByClass( WordCount.class );
		
		//2. mapper 클래스 지정
		job.setMapperClass( MyMapper.class );
		
		//3. reducer 클래스 지정
		job.setReducerClass( MyReducer.class );
		
		// 컴바이너 세팅
		job.setCombinerClass(MyReducer.class);
		
		//4. 출력
		job.setMapOutputKeyClass( StringWritable.class );
		
		//5. 출력 value 타입
		job.setMapOutputValueClass( NumberWritable.class );
		
		//6. 입력 파일 포멧 지정(생략 가능)
		job.setInputFormatClass( KeyValueTextInputFormat.class );
		
		//7. 출력 파일 포멧 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 리듀스 테스크 수 변경
		job.setNumReduceTasks(2);
		
		//8. 입력파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//9. 출력 디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
						
		// 실행
		job.waitForCompletion( true );
	}
}
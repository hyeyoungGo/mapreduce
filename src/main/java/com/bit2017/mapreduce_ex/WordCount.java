package com.bit2017.mapreduce_ex;

import java.io.IOException;
import java.util.StringTokenizer;

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

import com.bit2017.mapreduce.io.NumberWritable;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

public class WordCount {
	private static Log log = LogFactory.getLog(WordCount.class);
	
	public static class MyMapper extends Mapper<NumberWritable, Text, Text, NumberWritable> {
		private Text word = new Text();
		private static NumberWritable one = new NumberWritable(1L);
		
		
		@Override
		protected void setup(Mapper<NumberWritable, Text, Text, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> setup() called");
		}

		@Override
		protected void map(NumberWritable key, Text value, Mapper<NumberWritable, Text, Text, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> MyMapper.map() called");
			String line = value.toString();
			StringTokenizer tokenize = new StringTokenizer(line, "\r\n\t,|()<> ''");
			while (tokenize.hasMoreTokens()) {
				word.set(tokenize.nextToken().toLowerCase());
				context.write(word, one);
			}
		}
		
		
		@Override
		protected void cleanup(Mapper<NumberWritable, Text, Text, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> cleanup() called");
		}
		
		//run은 보통 Override하지 않는다.
		/*@Override
		public void run(Mapper<NumberWritable, Text, Text, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> run() called");
		}
		*/
	}	
	
	public static class MyReducer extends Reducer<Text, NumberWritable, Text, NumberWritable> {
		
		private NumberWritable sumWritable = new NumberWritable();
		
		@Override
		protected void reduce(Text key, Iterable<NumberWritable> values,
				Reducer<Text, NumberWritable, Text, NumberWritable>.Context context) throws IOException, InterruptedException {
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
		Job job = new Job( conf, "WordCount" );
		
		//1. job instance 초기화 작업
		job.setJarByClass( WordCount.class );
		
		//2. mapper 클래스 지정
		job.setMapperClass( MyMapper.class );
		
		//3. reducer 클래스 지정
		job.setReducerClass( MyReducer.class );
		
		//4. 출력
		job.setOutputKeyClass( Text.class );
		
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

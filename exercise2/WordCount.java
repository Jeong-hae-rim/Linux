package exercise2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.111.120:9000");

		Job job = Job.getInstance(conf, "WordCount");
		//잡 객체 생성.

		job.setJarByClass(WordCount.class);
		//잡 호출.setJarByClass : 드라이버 클래스의 클래스 객체를 주는 것.
		job.setMapperClass(WordCountMapper.class);
		//매퍼 머신에게 전달.
		job.setReducerClass(WordCountReducer.class);
        //리듀서 머신에게 전달. 아규먼트에게 객체를 보냄. 
		//리모트 프로시져 콜 : 
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class); // 키 데이터
		job.setOutputValueClass(IntWritable.class); // 밸류 데이터
		//주고받는 데이터 타입을 다르게 설정. 주고 받는 속도를 위해서.

		FileInputFormat.addInputPath(job, new Path("/edudata/fruits.txt")); //읽어서 워드 카운팅
		FileOutputFormat.setOutputPath(job, new Path("/result/exerout2")); //최종 결과를 이곳에 내보내겠다.
        //파일이 이미 존재하면 에러가 난다. 파일이 없어야지 실제 수행이 가능.
		
		job.waitForCompletion(true);
		System.out.println("처리가 완료되었습니다.");
	}
}
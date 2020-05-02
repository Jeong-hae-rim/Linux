package exercise1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	// 객체를 한 번만 생성해서 내용물만 바꿔서 write. 행 단위로 호출됨.

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 매개변수 세 개. 키, 밸류, 컨택스트
		System.out.println("MAP :" + key + " --- " + value);
		StringTokenizer itr = new StringTokenizer(value.toString());
		// 원하는 단위로 쪼갤 수 있는 API
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			if (word.getLength() >= 3 && word.getLength() <= 5) {
				context.write(word, one);
			}
		}
	}
}
package exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("\nREDUCE :" + key + " --- ");
		int sum = 0;
		for (IntWritable val : values) {
			  int data = val.get();
			 System.out.println(data + " "); 
			 sum += data; //워드마다 몇 번 돌지는 다르다.	
		}
		result.set(sum);
		context.write(key, result); //컨텍스트로 내보냄.
	}
}
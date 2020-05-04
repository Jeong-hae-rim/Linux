package exercise3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable outputvalue = new IntWritable(1);
	private Text outputkey = new Text();
	private String product;

	public String parser(Text text) {
	//public ProductMapper (Text text) {
		try {
			String[] columns = text.toString().split(" ");
			product = columns[1];
			System.out.println("?");

		} catch (Exception e) {
			System.out.println("!!" + e.getMessage());
		}
		return product; //원래 없었음
	}
	
	/*
	 * public String getProduct() { return product; }
	 * 
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//ProductMapper parser=new ProductMapper(value);
		//outputkey.set(parser.getProduct());
		
		System.out.println(key);
		System.out.println(parser(value));
		outputkey.set(parser(value));
		context.write(outputkey, outputvalue);
	}

}

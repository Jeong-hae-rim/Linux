package exercise3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ProductCount {
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
    	
    	Job job = Job.getInstance(conf, "ProductCount");
    	System.out.println("확인1");
    	
    	job.setJarByClass(ProductCount.class);
    	System.out.println("확인2");
    	
    	job.setMapperClass(ProductMapper.class);
    	System.out.println("확인3");
    	
    	job.setReducerClass(ProductReducer.class);
    	System.out.println("확인4");
    	
    	job.setInputFormatClass(TextInputFormat.class);
    	System.out.println("확인5");
    	//The method setInputFormatClass(Class<? extends InputFormat>) in the type Job is not applicable for the arguments (Class<TextInputFormat>)?
    	
    	job.setOutputFormatClass(TextOutputFormat.class);
    	System.out.println("확인6");
    	
    	job.setOutputKeyClass(Text.class);
    	System.out.println("확인7");
    	
	    job.setOutputValueClass(IntWritable.class);
	    System.out.println("확인8");
	    
	    FileInputFormat.addInputPath(job, new Path("/edudata/product_click.log"));
	    System.out.println("확인9");
	    
	    FileOutputFormat.setOutputPath(job, new Path("/result/exerout3"));
	    System.out.println("확인10");
	    
	    job.waitForCompletion(true);
	    System.out.println("처리가 완료되었습니다.");
	   
	}

}

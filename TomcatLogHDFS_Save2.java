package hdfsexam;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TomcatLogHDFS_Save2 {
	
	public static void main(String[] args) {
		
		String pathDir = "C:/haerim/eclipse-workspace/javaexam/.metadata/.plugins/org.eclipse.wst.server.core/tmp1/logs";
		String localFile = "C:/temp/tomcat_access_log.txt";
		
		try {
			File path = new File(pathDir);
			File[] fileList = path.listFiles();
			
			File file = new File(localFile);
			if (!file.exists()) {
				file.createNewFile();
			}else {
				System.out.println("파일이 존재합니다.");
				file.delete();
				file.createNewFile();
			}
			
			if (fileList.length > 0) {
				int i = 0;
				for(i=0; i < fileList.length;i++) {
					Files.write(file.toPath(), Files.readAllBytes(fileList[i].toPath()), StandardOpenOption.APPEND);
				}
			}
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
			FileSystem hdfs = FileSystem.get(conf);
			String fname = "tomcat_access_log.txt";
			File f = new File("C:/temp/" + fname);
			if (!f.exists()) {
				System.out.println("파일이 존재하지 않습니다.");
				return;
			}
			Path path2 = new Path("/edudata/" + fname);
			if(hdfs.exists(path2)) {
				hdfs.delete(path2, true);
			}
			long size = f.length();
			FileReader fr = new FileReader(f);
			char[] content = new char[(int) size];
			fr.read(content);
			FSDataOutputStream outStream = hdfs.create(path2);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outStream));
			writer.write(content);
			writer.close();
			outStream.close();
			fr.close();
			System.out.println("File Size : " + size);
		}catch (IOException e) {
			e.printStackTrace();
	    }
	}

}

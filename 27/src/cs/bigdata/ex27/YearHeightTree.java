package cs.bigdata.ex27;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class YearHeightTree {

	public static void main(String[] args) throws IOException {

		// Configuration to read file from hdfs
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/user/raphaelgavache/dataset/arbres.csv");
		
		InputStream in = new BufferedInputStream(fs.open(path));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			while (line !=null){
				// Process of the current line
				Tree.displayYearHeightFromLine(line);
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
	}
}

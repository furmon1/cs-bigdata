package cs.bigdata.ex28;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class ISDDisplayer {

	public static void main(String[] args) throws IOException {

		// Configuration to read file from hdfs
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/user/raphaelgavache/dataset/isd-history.txt");
		
		InputStream in = new BufferedInputStream(fs.open(path));
		
		int lineCounter = 0;

		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			while (line !=null){
				lineCounter += 1;
				
				if (lineCounter >= 23)
				{
					// Process of the current line
					printFormatedHistory(line);
				}
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
	
	public static void printFormatedHistory(String line){
		String USAF = line.substring(0, 6);
		String name = line.substring(13, 42);
		String FIPS = line.substring(43, 45);
		String altitude = line.substring(74, 81);
		System.out.println(String.format("%s %s %s %s", USAF, name, FIPS, altitude));
	}
}

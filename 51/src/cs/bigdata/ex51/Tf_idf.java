package cs.bigdata.ex51;

import org.apache.hadoop.conf.Configuration;

public class Tf_idf {
	public static void main(String[] args) throws Exception {
		// Connection a HDFS
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		
		// Step 1- word count
		WordCount.main(conf);
		
		// Step 2 - word count per doc
		WordCountDoc.main(conf);
		
		// Step 3 - compute tf-idf
		DocCountWord.main(conf);
		
		// Step 4 - sort by score
		SortKeyByValue.main(conf);
		
	}
}
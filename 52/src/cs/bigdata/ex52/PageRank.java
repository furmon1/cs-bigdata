package cs.bigdata.ex52;

import org.apache.hadoop.conf.Configuration;

public class PageRank {
	static int TOTAL_NOEUDS = 75879;
	static Double DAMPLING_FACTOR = 0.85;
	static int nIter = 100;
	
	public static void main(String[] args) throws Exception {
		// Connection a HDFS
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");

		// Step 1- Init info
		InitPR.main(conf);
		
		// Step 2 - Calcul du PageRank
		
		for (int i = 1; i <= nIter; i++)
		{
			CalculPR.main(conf, i);
		}
	}
}
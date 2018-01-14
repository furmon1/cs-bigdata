package cs.bigdata.ex53;

import org.apache.hadoop.conf.Configuration;

public class ArbresParis {
	public static void main(String[] args) throws Exception {
		// Connection a HDFS
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		
		// 1. Nombre d'arbre par espece
		CompteArbre.main(conf);
		
		// 2. Plus grand arbre de chaque type
		PlusGrandArbre.main(conf);
		
		// 3. Plus vieil arbre
		PlusVieilArbre.main(conf);
	}
}
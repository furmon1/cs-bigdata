package cs.bigdata.ex52;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CalculPR {
	
	public static class WCDMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		
	    /**
	     *     En entree : 
	     *     noeud1            -> valeurPG		"noeud3,noeud41,noeud12"
	     *
	     *     En sortie :
	     **(1) noeud1			-> "|noeud3,noeud41,noeud12"
	     **(2) noeud3			-> valeurPG		totalLiensNoeud1
	     *     ...
	     *     
	     *     Il y a de multiples sortie pour 2 entrée : soit une sortie par noeud cité pour
	     *     l'exemple ci-dessus une sortie de type *(1) et trois sorties de type *(2) pour 
	     *     les noeuds 3, 41, 12
	     */	
	    @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        int tIdx1 = value.find("\t");
	        int tIdx2 = value.find("\t", tIdx1 + 1);
	        
	        String page = Text.decode(value.getBytes(), 0, tIdx1);
	        String pageRank = Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1));
	        String liens_s = Text.decode(value.getBytes(), tIdx2 + 1, value.getLength() - (tIdx2 + 1));
	        String[] liens = liens_s.split(",");
	        // Sortie de type *(1)
	        context.write(new Text(page), new Text("|" + liens_s));
	        
	        // Sorties de type *(2)
	        for (String page2 : liens)
	        {
	        		context.write(new Text(page2), new Text(pageRank + "\t"  + liens.length));
	        }
	    }
	}

	public static class WCDReducer extends Reducer<Text, Text, Text, Text> {
		
	    /**
	     *     En entree : 
	     *     noeud1           -> ["|noeud3,noeud41,noeud12", "PRNoeud55    110", "PRNoeud100    3", ...]
	     *     
	     *     En sortie :
	     *     noeud1            -> valeurPG		"noeud3,noeud41,noeud12"
	     */
		@Override
	    public void reduce(Text page, Iterable<Text> values,
	    		Context context) throws IOException, InterruptedException 
		{
			String liens = "";
			String contenu = "";
			double sommePR = 0.0;
			
			for (Text value : values) 
			{
				contenu = value.toString();
				if (contenu.startsWith("|"))
				{
					// On est dans le cas "|noeud3,noeud41,noeud12" soit le cas *(1) du WCDMapper
	        			liens = contenu.substring(1);
				} else
				{
					// Cas *(2):  pagerank  ->  nombre_liens
					String [] pr_nliens = contenu.split("\t");
					double pr = Double.parseDouble(pr_nliens[0]);
					int nLiens = Integer.parseInt(pr_nliens[1]);
					
					sommePR += (pr / nLiens);
				}
			}
	        double newPR = PageRank.DAMPLING_FACTOR * sommePR + (1 - PageRank.DAMPLING_FACTOR);
	        context.write(page, new Text(newPR + "\t" + liens));
	    }
	  }
	  public static void main(Configuration conf, int numIter) throws Exception {
		    Job job = Job.getInstance(conf, "word count");
			
			// Path entree sortie
			Path path_input = new Path("/user/raphaelgavache/pageRankOutput" + numIter);
			Path path_output = new Path("/user/raphaelgavache/pageRankOutput" + (numIter + 1));
		    FileInputFormat.addInputPath(job, path_input);
		    FileOutputFormat.setOutputPath(job, path_output);
					    
		    // Classes map, reduce, programe
		    job.setJarByClass(CalculPR.class);
		    job.setMapperClass(WCDMapper.class);
		    job.setReducerClass(WCDReducer.class);

		    // Classes d'entree, sortie
		    job.setOutputKeyClass(TextInputFormat.class);
		    job.setOutputValueClass(TextOutputFormat.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);

		    job.waitForCompletion(true);
		  }
}

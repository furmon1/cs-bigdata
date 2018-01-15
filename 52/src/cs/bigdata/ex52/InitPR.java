package cs.bigdata.ex52;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class InitPR {
	
	public static class InitMapper extends Mapper<LongWritable, Text, Text, Text>
	{
	    /**     
	     * 	   En entree : 
	     *     noeud1                -> noeud2
	     *     
	     *     En sortie :
	     *     noeud1                -> noeud2
	     */
		
		@Override
		protected void map(LongWritable keyE, Text value, Context context) throws IOException, InterruptedException
		{			
			if (value.charAt(0) != '#') 
			{	
		        String[] noeuds = value.toString().split("\t");
	            context.write(new Text(noeuds[0]), new Text(noeuds[1]));
	        }
	    }
	}

	public static class InitReducer extends Reducer<Text, Text, Text, Text> 
	{
	    /**     
	     * 	   En entree : 
	     *     noeud1            -> [noeud3, noeud41, noeud12]
	     *     
	     *     En sortie :
	     *     noeud1            -> valeurPG		"noeud3,noeud41,noeud12"
	     *     Pour l'initialisation la valeur PG est Damping_factor/Total_noeuds
	     */
	    
	    @Override
	    public void reduce(final Text key, final Iterable<Text> values,
	            final Context context) throws IOException, InterruptedException
	    {
	    		// Calcul du premier PR
	        String liens = (PageRank.DAMPLING_FACTOR / PageRank.TOTAL_NOEUDS) + "\t";
	        Iterator<Text> iterator = values.iterator();
	        int n = 0;
	        while (iterator.hasNext()) 
	        {
        		if (n != 0)
        		{
        			liens += ",";
        		}
	        		liens += iterator.next().toString();
	        		n++;
	        }
	        context.write(key, new Text(liens));
	    }
	}
	
	public static void main(Configuration conf) throws Exception {
		  
		    Job job = Job.getInstance(conf, "word count");
		    
			Path path_input = new Path("/user/raphaelgavache/inputPageRank");
			Path path_output = new Path("/user/raphaelgavache/pageRankOutput1");
		    FileInputFormat.addInputPath(job, path_input);
		    FileOutputFormat.setOutputPath(job, path_output);
			
		    // Classes map, reduce, programe
		    job.setJarByClass(InitPR.class);
		    job.setMapperClass(InitMapper.class);
		    job.setReducerClass(InitReducer.class);

		    // Classes d'entree, sortie
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(TextInputFormat.class);
		    job.setOutputValueClass(TextOutputFormat.class);
		    
		    job.waitForCompletion(true);
		  }
}

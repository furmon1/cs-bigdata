package cs.bigdata.ex53;

import java.io.IOException;
import java.util.Iterator;

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


public class PlusGrandArbre {

	public static class PGAMapper extends Mapper<LongWritable, Text, Text, Text>
	{	
	    /**     
	     * 	   En entree : 
	     *     "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly"
	     *     
	     *     En sortie :
	     *     "Maclura" -> taille_Maclura
	     */
		
		@Override
		protected void map(LongWritable keyE, Text value, Context context) throws IOException, InterruptedException
		{
			String arbre = value.toString();
	        	String[] infos_arbre = arbre.split(";");
	        	
	        	// On ignore la premiere ligne et les hauteurs nulles
	        	if (!infos_arbre[0].equals("GEOPOINT") && !infos_arbre[6].equals("")) 
	        	{
	        		context.write(new Text(infos_arbre[2]), new Text(infos_arbre[6]));
	        	}
	    }
		
		public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    while (context.nextKeyValue()) {
		        map(context.getCurrentKey(), context.getCurrentValue(), context);
		    }
		    cleanup(context);
		}
	}

	public static class PGAReducer extends Reducer<Text, Text, Text, Text> {
	    /**     
	     * 	   En entree : 
	     *     "Maclura"            -> ["21.0", "184.0", "102.0"]
	     *     
	     *     En sortie :
	     *     "Maclura"            -> "184.0"
	     */
	    
	    @Override
	    public void reduce(final Text key, final Iterable<Text> values,
	            final Context context) throws IOException, InterruptedException {

	        double maxTaille = 0;
	        double tempTaille;
	        Iterator<Text> iterator = values.iterator();

	        while (iterator.hasNext()) {
	        		tempTaille = Double.parseDouble(iterator.next().toString());

	        	    if (maxTaille < tempTaille)
	        	    {
	        	    		maxTaille = tempTaille;
	        	    }
	        }
	        context.write(key, new Text(String.valueOf(maxTaille)));
	    }
	}
	
	public static void main(Configuration conf) throws Exception {
		  
		    Job job = Job.getInstance(conf, "word count");
		    
			Path path_input = new Path("/user/raphaelgavache/treeData");
			Path path_output = new Path("/user/raphaelgavache/treeOutput2");
		    FileInputFormat.addInputPath(job, path_input);
		    FileOutputFormat.setOutputPath(job, path_output);
			
		    // Classes map, reduce, programe
		    job.setJarByClass(PlusGrandArbre.class);
		    job.setMapperClass(PGAMapper.class);
		    job.setReducerClass(PGAReducer.class);

		    // Classes d'entree, sortie
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(TextInputFormat.class);
		    job.setOutputValueClass(TextOutputFormat.class);
		    
		    job.waitForCompletion(true);
		  }
}

package cs.bigdata.ex53;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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


public class CompteArbre {
	
	public static class CAMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		// Attributs
		private final static IntWritable one = new IntWritable(1);

		
	    /**     
	     * 	   En entree : 
	     *     "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly"
	     *     
	     *     En sortie :
	     *     "Maclura" -> 1
	     */
		
		@Override
		protected void map(LongWritable keyE, Text value, Context context) throws IOException, InterruptedException
		{
			String arbre = value.toString();
	        	String[] infos_arbre = arbre.split(";");
	        	
	        	// On ignore la premiere ligne
	        	if (!infos_arbre[0].equals("GEOPOINT")) 
	        	{
	        		context.write(new Text(infos_arbre[2]), one);
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

	public static class CAReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	    private IntWritable totalTreeCount = new IntWritable();

	    /**     
	     * 	   En entree : 
	     *     "Maclura"            -> ["1", "1", "1"]
	     *     
	     *     En sortie :
	     *     "Maclura"            -> "3"
	     */
	    
	    @Override
	    public void reduce(final Text key, final Iterable<IntWritable> values,
	            final Context context) throws IOException, InterruptedException {

	        int sum = 0;
	        Iterator<IntWritable> iterator = values.iterator();

	        while (iterator.hasNext()) {
	            sum += iterator.next().get();
	        }

	        totalTreeCount.set(sum);
	        context.write(key, totalTreeCount);
	    }
	}
	
	public static void main(Configuration conf) throws Exception {
		  
		    Job job = Job.getInstance(conf, "word count");
		    
			Path path_input = new Path("/user/raphaelgavache/treeData");
			Path path_output = new Path("/user/raphaelgavache/treeOutput1");
		    FileInputFormat.addInputPath(job, path_input);
		    FileOutputFormat.setOutputPath(job, path_output);
			
		    // Classes map, reduce, programe
		    job.setJarByClass(CompteArbre.class);
		    job.setMapperClass(CAMapper.class);
		    job.setReducerClass(CAReducer.class);

		    // Classes d'entree, sortie
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(TextInputFormat.class);
		    job.setOutputValueClass(TextOutputFormat.class);
		    
		    job.waitForCompletion(true);
		  }
}

package cs.bigdata.ex51;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount {
	
	public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		// Attributs
		private final static IntWritable one = new IntWritable(1);

		
	    /**     
	     * 	   En entree : 
	     *     callwild.txt                -> "Old longings nomadic leap, ..."
	     *     
	     *     En sortie :
	     *     old@callwild.txt            -> "1"
	     *     longings@callwild.txt       -> "1"
	     */
		
		@Override
		protected void map(LongWritable keyE, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			String mot;
			
			// On recupere le nom du fichier
	        String fichier = ((FileSplit) context.getInputSplit()).getPath().getName();
	        
	        while (itr.hasMoreTokens())
	        {
	        		// Mise en format de chaque mot
	        		mot = itr.nextToken();
	        		mot = mot.toLowerCase();  // lettres en minuscule
	        		mot = mot.replaceAll("[^\\w\\s]", "");  // Regex pour retirer les caracteres speciaux
	            context.write(new Text(mot + "@" + fichier), one);
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

	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	    private IntWritable totalWordCount = new IntWritable();

	    /**     
	     * 	   En entree : 
	     *     old@callwild.txt            -> ["1", "1", "1"]
	     *     
	     *     En sortie :
	     *     old@callwild.txt            -> "3"
	     */
	    
	    @Override
	    public void reduce(final Text key, final Iterable<IntWritable> values,
	            final Context context) throws IOException, InterruptedException {

	        int sum = 0;
	        Iterator<IntWritable> iterator = values.iterator();

	        while (iterator.hasNext()) {
	            sum += iterator.next().get();
	        }

	        totalWordCount.set(sum);
	        context.write(key, totalWordCount);
	    }
	}
	
	public static void main(Configuration conf) throws Exception {
		  
		    Job job = Job.getInstance(conf, "word count");
		    
			Path path_input = new Path("/user/raphaelgavache/input");
			Path path_output = new Path("/user/raphaelgavache/output1");
		    FileInputFormat.addInputPath(job, path_input);
		    FileOutputFormat.setOutputPath(job, path_output);
			
		    // Classes map, reduce, programe
		    job.setJarByClass(WordCount.class);
		    job.setMapperClass(WCMapper.class);
		    job.setReducerClass(WCReducer.class);

		    // Classes d'entree, sortie
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(TextInputFormat.class);
		    job.setOutputValueClass(TextOutputFormat.class);
		    
		    job.waitForCompletion(true);
		  }
}

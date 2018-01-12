package cs.bigdata.ex51;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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


public class WordCountDoc {
	
	public static class WCDMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		
	    /**
	     *     En entree : 
	     *     worse@callwild.txt           -> 6
	     *     worse@defoe-robinson-103.txt -> 22
	     *
	     *     En sortie :
	     *     callwild.txt -> worse=6
	     *     defoe.txt    -> worse=22
	     */	
	    @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] wordAndDocCounter = value.toString().split("\t");
	        String[] wordAndDoc = wordAndDocCounter[0].split("@");
	        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));
	    }
	}

	public static class WCDReducer extends Reducer<Text, Text, Text, Text> {
		
	    /**
	     *     En entree : 
	     *     callwild.txt           -> ["worse=6", "and=1464", ...]
	     *     
	     *     En sortie :
	     *     worse@callwild.txt            -> "6;31778"
	     */
		@Override
	    public void reduce(Text docID, Iterable<Text> mot_compte_it,
	    		Context context) throws IOException, InterruptedException {
	    	
	        int totalMots = 0;
	        // Dictionnaire pour stocker les mots et leur compte 
	        Map<String, Integer> mot_compte = new HashMap<String, Integer>();
	        
	        for (Text val : mot_compte_it) {
	            String[] wordCounter = val.toString().split("=");
	            mot_compte.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
	            totalMots += Integer.parseInt(val.toString().split("=")[1]);
	        }
	        
	        for (String mot : mot_compte.keySet()) {
	            context.write(
	            		new Text(mot + "@" + docID.toString()),
	            		new Text(mot_compte.get(mot) + ";" + totalMots)
	            		);
	        }
	    }
	  }
	  public static void main(Configuration conf) throws Exception {
		    Job job = Job.getInstance(conf, "word count");
			
			// Path entree sortie
			Path path_input = new Path("/user/raphaelgavache/output1");
			Path path_output = new Path("/user/raphaelgavache/output2");
		    FileInputFormat.addInputPath(job, path_input);
		    FileOutputFormat.setOutputPath(job, path_output);
					    
		    // Classes map, reduce, programe
		    job.setJarByClass(WordCountDoc.class);
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

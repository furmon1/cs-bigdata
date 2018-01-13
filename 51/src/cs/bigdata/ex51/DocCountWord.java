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


public class DocCountWord
{
		
	public static class WCTDMapper extends Mapper<LongWritable, Text, Text, Text>
	{
	    /**
	     *     En entree : 
	     *     candle@callwild.txt            -> "2;31778"
	     *
	     *     En sortie :
	     *     candle  -> "callwild.txt=2;31778"
	     */	
		@Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
	        String[] motDoc_compteurs = value.toString().split("\t");
	        String[] motDoc = motDoc_compteurs[0].split("@");
	        context.write(new Text(motDoc[0]), new Text(motDoc[1] + "=" + motDoc_compteurs[1]));
	    }
	}
	
	public static class WCTDReducer extends Reducer<Text, Text, Text, Text> 
{
		
	    /**
	     *     En entree : 
	     *     candle  -> "callwild.txt=2;31778"
	     *     
	     *     En sortie :
	     *     candle@callwild.txt            -> "tf-idf"
	     */
		@Override
	    public void reduce(Text key, Iterable<Text> values,
	    		Context context) throws IOException, InterruptedException 
		{
			
	        int nombreFichiersTextes = 2;
	        // frequence totale du mot
	        int frequenceMot = 0;
	        Map<String, String> doc_freqs = new HashMap<String, String>();
	        
	        for (Text val : values) 
	        {
	            String[] doc_frequence = val.toString().split("=");
	            frequenceMot++;
	            doc_freqs.put(doc_frequence[0], doc_frequence[1]);
	        }
	        for (String document : doc_freqs.keySet()) 
	        {
	            String[] frequence_totalMot = doc_freqs.get(document).split(";");
	 
	            // calcul du tf-idf
	            double tf = Double.valueOf(Double.valueOf(frequence_totalMot[0])
	                    / Double.valueOf(frequence_totalMot[1]));	 
	            double idf = (double) nombreFichiersTextes / (double) frequenceMot;	 
	            Double tfIdf = nombreFichiersTextes == frequenceMot ?
	                    tf : tf * Math.log10(idf);
	 
	            context.write(new Text(key + "@" + document), new Text(tfIdf.toString()));
	        	}
	    }
	}

	public static void main(Configuration conf) throws Exception {
		  
	    Job job = Job.getInstance(conf, "word count");
	    
		Path path_input = new Path("/user/raphaelgavache/output2");
		Path path_output = new Path("/user/raphaelgavache/output3");
	    FileInputFormat.addInputPath(job, path_input);
	    FileOutputFormat.setOutputPath(job, path_output);
		
	    // Classes map, reduce, programe
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(WCTDMapper.class);
	    job.setReducerClass(WCTDReducer.class);

	    // Classes d'entree, sortie
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(TextInputFormat.class);
	    job.setOutputValueClass(TextOutputFormat.class);
	    
	    job.waitForCompletion(true);
	  }
}


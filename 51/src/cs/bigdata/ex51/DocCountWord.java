package cs.bigdata.ex51;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;

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
	     *     worse@callwild.txt            -> "6;31778"
	     *
	     *     En sortie :
	     *     worse  -> "callwild.txt=6;31778"
	     */	
		@Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
	        String[] motCompteurs = value.toString().split("\t");
	        String[] motDoc = motCompteurs[0].split("@");
	        String mot = motDoc[0];
	        String doc = motDoc[1];
	        String compteurs = motCompteurs[1];
	        context.write(new Text(mot), new Text(doc + "=" + compteurs));
	    }
	}
	
	public static class WCTDReducer extends Reducer<Text, Text, Text, Text> 
{
		
	    /**
	     *     En entree : 
	     *     callwild.txt           -> ["worse=6", "and=1464", ...]
	     *     
	     *     En sortie :
	     *     worse@callwild.txt            -> "6;31778"
	     */
		@Override
	    public void reduce(Text key, Iterable<Text> values,
	    		Context context) throws IOException, InterruptedException 
		{
			
	        int numberOfDocumentsInCorpus = 2;
	        // total frequency of this word
	        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
	        Map<String, String> tempFrequencies = new HashMap<String, String>();
	        
	        Map<String, Double> tfIdfs = new HashMap<String, Double>();
	        for (Text val : values) 
	        {
	            String[] documentAndFrequencies = val.toString().split("=");
	            numberOfDocumentsInCorpusWhereKeyAppears++;
	            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
	        }
	        for (String document : tempFrequencies.keySet()) 
	        {
	            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split(";");
	 
	            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
	            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
	                    / Double.valueOf(wordFrequenceAndTotalWords[1]));
	 
	            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
	            double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;
	 
	            //given that log(10) = 0, just consider the term frequency in documents
	            Double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereKeyAppears ?
	                    tf : tf * Math.log10(idf);
	 
	            context.write(new Text(key + "@" + document), new Text(tfIdf.toString()));
	            tfIdfs.put(key + "@" + document, tfIdf);
	        	}
	        
	        // Trie des 20 plus grands tf-idf
	        Map<String, Integer> result = unsortMap.entrySet().stream()
	                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
	                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
	                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
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


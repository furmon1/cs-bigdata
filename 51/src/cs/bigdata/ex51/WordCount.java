package cs.bigdata.ex51;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	public static class WCMapper extends Mapper<Text, Text, Text, IntWritable>
	{
		public void map(Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
	        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	        
	        while (itr.hasMoreTokens())
	        {
	            context.write(new Text(itr.nextToken() + "@" + fileName), new IntWritable(1));
	        }
	    }
	}

	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values, Context context
	                       ) throws IOException, InterruptedException 
	    {
	      int sum = 0;
	      for (IntWritable val : values)
	      {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }
	  public static void main(String[] args) throws Exception {
		  
			// Configuration to read file from hdfs
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS","hdfs://localhost:9000");
			Path path_input = new Path("/user/raphaelgavache/input");
			Path path_output = new Path("/user/raphaelgavache/output1");
			
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(WordCount.class);
		    job.setMapperClass(WCMapper.class);
		    job.setCombinerClass(WCReducer.class);
		    job.setReducerClass(WCReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, path_input);
		    FileOutputFormat.setOutputPath(job, path_output);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}

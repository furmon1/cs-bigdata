package cs.bigdata.ex53;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PlusVieilArbre {

	public static class CustomKey implements WritableComparable<CustomKey> {
		
		protected String arrondissement = new String();
		protected int annee;

		public CustomKey() {
		}

		public CustomKey(String arr, int anneePlantation) {
			arrondissement = arr;
			annee = anneePlantation;
		}

		@Override
		public void readFields(DataInput di) throws IOException {
			arrondissement = di.readUTF();
			annee = di.readInt();
		}

		@Override
		public void write(DataOutput d) throws IOException {
			d.writeUTF(arrondissement);
			d.writeInt(annee);
		}

		@Override
		public int compareTo(CustomKey ck) {
			return annee - ck.annee;
		}
	}
		
	public static class PVAMapper extends Mapper<LongWritable, Text, PlusVieilArbre.CustomKey, Text>
	{
		
		@Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String arbre = value.toString();
        	String[] infos_arbre = arbre.split(";");
        	String annee = infos_arbre[5];
        	String arrondissement = infos_arbre[1];
        	
        	// On ignore la premiere ligne et l'annee de plantation et l'arrondissement sont non nuls
        	if (!infos_arbre[0].equals("GEOPOINT") && !annee.equals("") && !arrondissement.equals("")) 
        	{
    	        context.write(
    	        		new CustomKey(arrondissement, Integer.parseInt(annee)), 
    	        		new Text(arrondissement + " -> " + annee));
        	}
	    }
	}
	
	public static void main(Configuration conf) throws Exception {
		  
	    Job job = Job.getInstance(conf, "word count");
	    
		Path path_input = new Path("/user/raphaelgavache/treeData");
		Path path_output = new Path("/user/raphaelgavache/treeOutput3");
	    FileInputFormat.addInputPath(job, path_input);
	    FileOutputFormat.setOutputPath(job, path_output);
		
	    // Classes map, reduce, programe
	    job.setJarByClass(PlusVieilArbre.class);
	    job.setMapperClass(PVAMapper.class);

	    // Classes d'entree, sortie
	    job.setMapOutputKeyClass(CustomKey.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(TextOutputFormat.class);
	    
	    job.waitForCompletion(true);
	  }
}

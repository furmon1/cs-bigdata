package cs.bigdata.ex51;

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


public class SortKeyByValue {

public static class CustomKey implements WritableComparable<CustomKey> {
	
	protected String key1 = new String();
	protected Double tf_idf;

	public CustomKey() {
	}

	public CustomKey(String nom, Double tf_idf2) {
		key1 = nom;
		tf_idf = tf_idf2;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		key1 = di.readUTF();
		tf_idf = di.readDouble();
	}

	@Override
	public void write(DataOutput d) throws IOException {
		d.writeUTF(key1);
		d.writeDouble(tf_idf);
	}

	@Override
	public int compareTo(CustomKey ck) {
		return ck.tf_idf.compareTo(tf_idf);
	}
}
	
public static class SortMapper extends Mapper<LongWritable, Text, SortKeyByValue.CustomKey, Text>
{
    /**
     *     En entree : 
     *     worse@callwild.txt            -> tf-idf score
     *
     *     En sortie :
     *     tf-idf@score  -> tf-idf
     */	
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
        String[] key_value = value.toString().split("\t");
        String nom = key_value[0];
        Double tf_idf = Double.parseDouble(key_value[1]);
        context.write(new CustomKey(nom, tf_idf), new Text(nom + " -> " + key_value[1]));
    }
}

public static void main(Configuration conf) throws Exception {
	  
    Job job = Job.getInstance(conf, "word count");
    
	Path path_input = new Path("/user/raphaelgavache/output3");
	Path path_output = new Path("/user/raphaelgavache/output4");
    FileInputFormat.addInputPath(job, path_input);
    FileOutputFormat.setOutputPath(job, path_output);
	
    // Classes map, reduce, programe
    job.setJarByClass(SortKeyByValue.class);
    job.setMapperClass(SortMapper.class);

    // Classes d'entree, sortie
    job.setMapOutputKeyClass(CustomKey.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextOutputFormat.class);
    
    job.waitForCompletion(true);
  }

}

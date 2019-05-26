import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver{
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		//we have two MR, so two jobs
		Configuration conf1 = new Configuration();
		conf1.set("Num_Gram", args[2]);
		Job job1 = Job.getInstance();
		job1.setJobName("NGram");
		job1.setJarByClass(Driver.class);

		job1.setMapperClass(NGramLibBuilder.NGramMapper.class);
		job1.setReducerClass(NGramLibBuilder.NGramReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		TextInputFormat.setInputPaths(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		//the output of job1 is the input of job2
		Configuration conf2 = new Configuration();
		conf2.set("threahold", args[3]);
		conf2.set("top_k", args[4]);

		DBConfiguration.configureDB(
			conf2, 
			"com.mysql.jdbc.Driver",
			"jdbc:mysql://ip_address:port/test",
			"root",
			"password"
			);


		Job job2= Job.getInstance();
		job2.setJobName("LanguageModel");
		job2.setJarByClass(Driver.class);

		job2.addArchiveToClassPath(new Path("path_to_ur_connector"));
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setMapperClass(LanguageModel.Map.class);
		job2.setReducerClass(LanguageModel.Reduce.class);

		job2.setOutputKeyClass(DBOutputWritable.class);
		job2.setOutputValueClass(NullWritable.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(DBOutputFormat.class);

		DBOutputFormat.setOutput(
			job2,
			"output",
			new String[] {"starting_phrase", "following_word", "count"};
			);

		TextInputFormat.setInputPaths(job2, args[1]);
		job2.waitForCompletion(true);
	}
}
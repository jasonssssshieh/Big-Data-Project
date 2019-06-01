import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextOutputFormat;

public class DataDividerByUser{
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		//Input: 1,10001,5.0
		//output: 1, 10001:5.0 (Int, string)
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] userMoiveRatings = value.trim().split(",");
			if(userMoiveRatings.size() < 3) return;
			int userID = Integer.parseInt(userMoiveRatings[0]);
			String movieID = userMoiveRatings[1];
			String rating = userMoiveRatings[2];
			context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		//intput: 1, 10001:5.0 (Intwritable, Text)
		//output: key: 1, value: 10001:4,10002:5....
		@Override
		public void reduce(IntWritable key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			while(values.iterator().hasNext()){
				sb.append("," + values.iterator().next());
			}
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(DataDividerByUser.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValue(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPaths(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}

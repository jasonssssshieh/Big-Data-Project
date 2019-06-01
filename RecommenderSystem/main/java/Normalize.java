 import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//in the last step, we get {movieA:movieB, relation} = {1:1, 2}这样的结果
//这一步 我们需要将matrix进行归一化处理,得到这样的格式=> {movieB, movieA=relation}

public class Normalize{
	public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
		//input: movieA:movieB \t value 4(所以这里已经serialization过了?)
		//output: {movieA, movieB:4}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] movie_relation = value.toString().trim().split("\t");
			String[] movies = movie_relation[0].split(":");
			context.write(new Text(movies[0]), new Text(movies[1] + ":" + movie_relation[1]));
		}
	}

	public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//input: Key = movieA, Value = {movieB:relation, movieC:relation, ......}
			//output: 
			int total = 0;
			Map<String, Integer> mp = new HashMap<String, Integer>();
			while(values.iterator.hasNext()){
				String[] movie_relation = values.iterator.next().toString().trim().split(":");
				//[movieB, relation]
				int relation = Integer.parseInt(movie_relation[1]);
				total += relation;
				//统计总个数
				mp.put(movie_relation[0], relation);
			}
			for(Map.Entry<String, Integer> entry : mp.entrySet()){
				String outputKey = entry.getKey();
				String outputValue = key.toString().trim() + "=" + (double)entry.getValue()/total;
				context.write(new Text(outputKey), new Text(outputValue));
			}
			//最后的output格式就是{key: movieB, value: movieA = 2/4}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setMapperClass(NormalizeMapper.class);
		job.setReducerClass(NormalizeReducer.class);

		job.setJarByClass(Normalize.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPaths(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
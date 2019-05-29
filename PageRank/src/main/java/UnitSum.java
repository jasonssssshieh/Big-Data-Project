import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum{
	public static class PassMapper extends Mapper <Object, Text, Text, Text>{

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			//input: pageA 0.152
			//       pageB 0.233
			String[] pageSubRank = value.toString().split("\t");
			double subRank = Double.parseDouble(pageSubRank[1]);
			context.write(new Text(pageSubRank[0]), new DoubleWritable(subRank));
		}
	}

	//the second mapper, so it will also add the beta * pr_0 into the page rank
	public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		float beta;
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			beta = conf.getFloat("beta", 0.2f);
		}

		@Overrid
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
			//pageA 0.152 传进来的是网站ID和 他原本的page rank
			String[] pageSubRank = value.toString().split("\t");
			double betaRank = Double.parseDouble(pageSubRank[1]) * beta;
			context.write(new Text(pageSubRank[0]), new DoubleWritable(betaRank));
		}
	}

	public static class SumReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException {
			double sum = 0;
			for(DoubleWritable value : values){
				sum += value.get();
			}
			DecimalFormat df = new DecimalFormat("#.00000");
			sum = Double.valueOf(df.format(sum));
			context.write(key, DoubleWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setFloat("beta", Float.parseFloat(args[3]));
		//由于现在这里有2个mapper了,那么接下来就需要有multipleInputs 和ChainMapper 的设置
		Job job = Job.getInstance(conf);
		job.setJarByClass(UnitSum.class);

		ChainMapper.addMapper(job, PassMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, BetaMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);

		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BetaMapper.class);

		FileOutputFormat.addInputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
}












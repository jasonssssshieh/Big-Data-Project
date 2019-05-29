import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.IntputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOEXception;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication{
	public static class TransitionMapper extends Mapper<Object, Text, Text, Text>{
		@Override
		public void map(Object key, Text values, Context context) throws IOEXception, InterruptedException{
			String line = values.toString().trim();
			String[] fromTo = line.split("\t");

			if(fromTo.length == 1 || fromTo[1].trim().equals("")){
				return;
			}
			String from = fromTo[0];
			//the input format: "1 => 2,3,4,5,6"
			String tos = fromTo[1].split(",");
			for(String to : tos){
				context.write(new Text(from), new Text(to + "="+double(1/tos.length)));
				//[pagenumber, values]=> [1, 4=1/4]
				// [1, 2=1/4].....
			}
		}
	}

	public static class PRMapper extends Mapper<Object, Text, Text, Text>{
		@Override
		public void map(Object key, Text value, Context context){
			String prob = value.toString().trim();
			context.write(new Text(prob[0]), new Text(prob[1]));//website and prob
		}
	}

	public static class MultiplicationReducer extends Reducer <Text, Text, Text, Text>{
		//[Key=pageNumber, List of Values: 2=1/4, 3=1/4, 4=1/4, 5=1/5, 1/6012] 最后一个是initialized value
		float beta;
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			beta = conf.getFloat("beta", 0.2f);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOEXception, InterruptedException {
			List<String> trasitionUnit = new ArrayList<String>();
			double prob0 = 0;
			for(Text value : values){
				if(value.toString().contains("=")){
					trasitionUnit.add(value.toString());
				}else{
					prob0 = Double.parseDouble(value.toString());
				}
			}
			for(String unit : trasitionUnit){
				String outputKey = unit.split("=")[0];
				double relation = Double.parseDouble(unit.split("=")[1]);
				// 1- beta
				String outputValue = String.valueOf(relation*prob0*(1-beta));
				context.write(new Text(outputKey), new Text(outputValue));
			}
		}
	}

	public static void main(String[] args) throws IOEXception, InterruptedException {
		Configuration conf = new Configuration();
		conf.setFloat(beta, Float.parseFloat(args[3]));
		Job job = Job.getInstance(conf);
		job.setJarByClass(UnitMultiplication.class);

		ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

		job.setReduceClass(MultiplicationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
}
















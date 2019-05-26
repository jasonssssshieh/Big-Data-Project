import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.*;
import javafx.util.Pair;

//this is the second step (the second MR in our process)
//previous we've had the output from the first MR with the  Text and IntWritable (input)
//now we will select the top K elements and write into our database
public class LanguageModel{
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		int threshold;//threshold for the Top k
		@Override
		public void setup(Context context){
			Configuraiton conf = context.getConfiguraiton();
			threshold = conf.getInt("threshold", 5);//少于五次统计数的就不予以考虑
		}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			if(value == null) return;
			//text format: this is cool\t20
			String line = value.toString().trim();
			if(line.length == 0) return;
			String[] wordsAndCount = line.split("\t");
			if(wordsAndCount.length < 2) return;
			String[] words = wordsAndCount[0].split("\\s+");
			int count = Integer.ValueOf(wordsAndCount[1]);

			if(count < threshold) return;//就是有一些特别小的count 直接就不要了 因为本身概率就很低

			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < wordsAndCount[0].length - 1; ++i){
				sb.append(wordsAndCount[0][i]);
				sb.append(" ");
			}
			sb = sb.trim();
			String StartingPhrase = sb.toString();
			String FollowingPhrase = wordsAndCount[0][wordsAndCount[0].length-1];
			String CountString = Integer.toString(count);
			context.write(new Text(StartingPhrase), new Text(FollowingPhrase + "=" + CountString));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable>{
		int top_k;
		//top k algorithm
		@Override
		public void setup(Context context){
			Configuraiton conf = context.getConfiguraiton();
			top_k = conf.getInt("top_k", 5);
		}

		class pairComparator implements Comparator<Pair<Integer, String>>{
			public bool compare(Pair<Integer, String> a, Pair<Integer, String> b){
				if(a.getValue() < b.getValue) return true;
				return false;
			}
		}
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context){
			//format: hi, <boy=10, girl=20, hello=30>
			PriorityQueue<Pair<Integer, String>> pq = new PriorityQueue(new pairComparator());
			for(Text val : values){
				String valStr = val.toString().trim();
				String[] words = valStr.split("=");
				String word = words[0].trim();
				int count = Integer.parseInt(words[1].trim());
				pq.add(new Pair<Integer, String> (count, word));
				if(pq.size > top_k){
					pq.poll();
				}
			}
			//<30, hello>, <20, boy>, <20, girl>
			while(pq.size()){
				Pair<Integer, String> p = pq.poll();
				context.write(new DBOutputWritable(key, p.getValue().toString, p.getKey()), NullWritable.get());
			}
		}
	}
}
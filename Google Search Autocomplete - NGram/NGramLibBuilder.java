import java.io.IOExeption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.inpit.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibBuilder{
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        int Num_Gram;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            Num_Gram = conf.getInt("Num_Gram", 6);//default value is 5
        }

        //map method for MR to build the NGramLibBuilder
        //context contains everything from the args[], including the configurations and etc
        //the input format for the mapper: file, and then read it.
        // and we will didive the input string as the NGram, and the value is 1 for each key-value pair
        @Override
        public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException{
            String line = value.toString();
            line = line.trim().toLowerCase();
            line = line.replaceAll("[^a-z]", " ");
            String [] words = line.split("\\s+");//split by ' ', '\t'...ect
            
            if(words.length < 2) {return;}
            StringBuilder sb;
            //now we are building the N gram lib, 
            for(int i = 0; i < words.length-1; ++i){//for each words
                sb = new StringBuilder();
                sb.append(words[i]);
                for(int j = 1; i + j < words.length && j < Num_Gram; ++j){//try to find all the possible combination of the continuous following words
                    sb.append(" ");
                    sb.append(words[i+j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        //reduce method
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int count = 0;
            for(IntWritable val : values){
                count += val.get();
            }
            context.write(key, new IntWritable(count));
        }
    }
}

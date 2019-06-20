import java.io.IOException;
import java.util.StringTokenizer;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

  // add code here
  public final static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
    private static  IntWritable sum = new IntWritable(0);
    private static IntWritable user = new IntWritable(0);

    @Override
    public void map (LongWritable key, Text value, Context context)throws IOException, InterruptedException{
      String[] tokens = value.toString().split(",", -1);
      for(int i = 1; i < tokens.length; i++){
        user.set(i);
        if( tokens[i].equals(""))
          sum.set(0);
        else
          sum.set(1);
        context.write(user,sum);
      }
    }
  }

  public final static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

    private static  IntWritable res = new IntWritable(0);

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
       int sum = 0;
       for (IntWritable val: values){
         sum += val.get();
       }
       res.set(sum);
       context.write(key,res);
    }

  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    Job job = new Job(conf, "Task3");
    job.setJarByClass(Task3.class);

    // add code here
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

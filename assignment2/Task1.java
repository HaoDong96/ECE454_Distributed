import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;


public class Task1 {
    // add code here
    // Mapper: emits (token, 1) for every word occurrence.
    public static final class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Text USER_NO = new Text();
        private static final Text MOVIE = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] entry = value.toString().split(",", -1);
            StringBuilder res = new StringBuilder();

            MOVIE.set(entry[0]);

            int max = 0;
            for (int i = 1; i < entry.length; i++) {
                if (entry[i].equals("")) continue;
                if (Integer.valueOf(entry[i]) > max)
                    max = Integer.valueOf(entry[i]);
            }
            for (int i = 1; i < entry.length; i++) {
                if (entry[i].equals("")) continue;
                if (Integer.valueOf(entry[i]) == max) {
                    if (!res.toString().equals(""))
                        res.append(",");
                    res.append(i);
                }
            }
            USER_NO.set(String.valueOf(res));
            context.write(MOVIE, USER_NO);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "Task1");
        job.setJarByClass(Task1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
//        job.setReducerClass(MyReducer.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
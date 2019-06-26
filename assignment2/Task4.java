

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {

    // add code here

    // Mapper1: emit (movie, ratings[]) for each movie.
    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, ArrayPrimitiveWritable> {
        public static Text movie = new Text();
        public static ArrayPrimitiveWritable ratings = new ArrayPrimitiveWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",", -1);
            int[] r = new int[tokens.length - 1];
            movie.set(tokens[0]);

            for (int i = 1; i < tokens.length; i++) {
                r[i - 1] = tokens[i].equals("") ? 0 : Integer.parseInt(tokens[i]);
            }

            ratings.set(r);
            context.write(movie, ratings);
        }
    }

    // Reducer1: emit what it gets
    public static class MyReducer1 extends Reducer<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }


    // Reducer2: emit <(movie1, movie2),similarity>
    public static class MyReducer2 extends Reducer<Text, ArrayPrimitiveWritable, Text, IntWritable> {
        private static HashMap<String, int[]> movieRatingsMap = new HashMap<>();
        private Text moviePair = new Text();
        private IntWritable similarity = new IntWritable(0);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            Path path = new Path("job1Output/part-r-00000");
            Text key = new Text();
            ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
            SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));

            while (reader.next(key, value)) {
                movieRatingsMap.put(key.toString(), (int[]) value.get());
            }
            reader.close();
        }

        @Override
        protected void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
            String thisMovie = key.toString();
            int[] ratings = (int[]) values.iterator().next().get();

            for (Map.Entry<String, int[]> movieInMap : movieRatingsMap.entrySet()) {

                if (thisMovie.compareTo(movieInMap.getKey()) < 0) {
                    moviePair.set(thisMovie + "," + movieInMap.getKey());
                    int sum = 0;
                    for (int i = 0; i < ratings.length; i++) {
                        if (ratings[i] == movieInMap.getValue()[i] && ratings[i] != 0)
                            sum++;
                    }
                    similarity.set(sum);
                    context.write(moviePair, similarity);
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job1 = new Job(conf, "Task4");
        job1.setJarByClass(Task4.class);
        // add code here
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(ArrayPrimitiveWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setNumReduceTasks(1);
        TextInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job1, new Path("job1Output"));

        // Delete the output directory if it exists already.
        Path outputDir = new Path("job1Output");
        FileSystem.get(conf).delete(outputDir, true);

//        DistributedCache.addCacheFile(new Path("mediumFile").toUri(), job1.getConfiguration());

        Job job2 = new Job(conf, "Task4");
        job2.setJarByClass(Task4.class);

        job2.setMapperClass(MyMapper1.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job2, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

        System.exit(job1.waitForCompletion(true) && job2.waitForCompletion(true) ? 0 : 1);
    }
}

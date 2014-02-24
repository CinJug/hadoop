package com.tailtarget.bigdata.examples.hadoop;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This MapReduce implementation show how you can reuse Writable objects.
 * It gets a web access log file, finds an url, extract the url domain and
 * counts how many visits that domain had.
 * See README file to learn how to execute this example.
 * 
 */
public class Listing2 extends Configured implements Tool {

    public static class Mapp extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text domain = new Text();
        private static final IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String line = value.toString();
                // There are more efficient ways to parse a String than split.
                // As an exercise, try to change techniques and observe how execution time changes
                String[] parts = line.split(" ");
                String host = new URL(parts[2]).getHost();
                domain.set(host);
                context.write(domain, one);
            } catch (MalformedURLException e) {
            }

        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable counter = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable value : values) {
                count = count + value.get();
            }

            counter.set(count);
            context.write(key, counter);


        }
    }

    /**
     *
     * @param args
     *
     */
    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        config.set("fs.default.name", args[0]);
        config.set("mapred.map.tasks.speculative.execution", "false");
        config.set("mapred.reduce.tasks.speculative.execution", "false");
        config.set(RuntimeParameters.TMP_DIR, "/tmp/demos-mr/");

        int result = ToolRunner.run(config, new Listing2(), args);
        if (result != 1) {
            System.err.println("Hadoop process finished with error. ");
            System.exit(1);
        }

    }

    @Override
    public int run(String[] args) throws Exception {

        // Finds logs that need processing
        FileSystem fs = FileSystem.get(getConf());
        Path path = new Path(args[1]);
        FileStatus[] files = fs.listStatus(path);
        int rc = 0;
        if (files != null && files.length > 0) {

            Job job = new Job();
            job.setJarByClass(Listing2.class);

            job.setJobName("Listing2");

            for (FileStatus file : files) {
                FileInputFormat.addInputPath(job, file.getPath());
            }

            // We are simply saving the output to /tmp/demos, so you can check the results
            // You can adapt this to your own evironment
            FileOutputFormat.setOutputPath(job, new Path("/tmp/demos/" + System.currentTimeMillis()));

            job.setMapperClass(Listing2.Mapp.class);
            job.setReducerClass(Listing2.Reduce.class);
            job.setCombinerClass(Listing2.Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.submit();

            rc = (job.waitForCompletion(true)) ? 1 : 0;


        }
        return rc;

    }
}

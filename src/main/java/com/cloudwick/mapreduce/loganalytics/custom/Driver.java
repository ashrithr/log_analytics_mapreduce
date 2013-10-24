package com.cloudwick.mapreduce.loganalytics.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * Illustrates use of custom input file format, record reader, partitioner, writable comparable
 */
public class Driver extends Configured implements Tool {
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("custom.Driver <inDir> <outDir> <numReducers>");
      ToolRunner.printGenericCommandUsage(System.out);
      System.out.println("");
      return -1;
    }

    System.out.println(Arrays.toString(args));

    Job job = new Job(getConf(), "custom log analysis");
    job.setJarByClass(Driver.class);
    job.setMapperClass(CustomMapper.class);
    job.setReducerClass(CustomReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(HTTPLogInputFormat.class);
    job.setPartitionerClass(UrlPartitioner.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setNumReduceTasks(Integer.parseInt(args[2]));
    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Driver(), args);
    System.exit(res);
  }
}

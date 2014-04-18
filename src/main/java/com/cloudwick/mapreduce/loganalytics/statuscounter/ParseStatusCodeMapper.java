package com.cloudwick.mapreduce.loganalytics.statuscounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * parses the line using regex and emits status code of the request
 */
public class ParseStatusCodeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

  public static final Pattern httplogPattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"([^\\s]+) (/[^\\s]*) HTTP/[^\\s]+\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"$");

  public static enum LOG_PROCESSOR_COUNTER {
    MALFORMED_RECORDS,
    PROCESSED_RECORDS,
  }

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    Matcher matcher = httplogPattern.matcher(value.toString());
    if (matcher.matches()) {
      context.getCounter(LOG_PROCESSOR_COUNTER.PROCESSED_RECORDS).increment(1);
      int responseCode = Integer.parseInt(matcher.group(6));
      context.write(new IntWritable(responseCode), new IntWritable(1));
    } else {
      context.getCounter(LOG_PROCESSOR_COUNTER.MALFORMED_RECORDS).increment(1);
    }
  }
}
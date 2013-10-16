package com.cloudwick.mapreduce.loganalytics.custom;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * custom record reader for HTTP log events
 *
 * @author ashrith
 */
public class HTTPLogRecordReader extends RecordReader<LongWritable, LogWritable> {
  LineRecordReader lineReader;
  LogWritable value;

  /*
   * read lines of text
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext attempt)
      throws IOException, InterruptedException {
    lineReader = new LineRecordReader();
    lineReader.initialize(inputSplit, attempt);

  }

  /*
   * custom parsing of the log entries of the input data
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!lineReader.nextKeyValue())
    {
      return false;
    }

    Pattern httpLogPattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+)" +
        " \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"$");

    Matcher matcher = httpLogPattern.matcher(lineReader.getCurrentValue().toString());
    if (!matcher.matches()) {
      System.out.println("Bad Record:"+ lineReader.getCurrentValue());
      return nextKeyValue();
    }

    String userIP = matcher.group(1);
    String timestamp = matcher.group(4);
    String request = matcher.group(5);

    int status = Integer.parseInt(matcher.group(6));
    int bytes = Integer.parseInt(matcher.group(7));

    String browser = matcher.group(9);

    value = new LogWritable();
    value.set(userIP, timestamp, request, browser, status, bytes);
    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return lineReader.getCurrentKey();
  }

  @Override
  public LogWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }
}

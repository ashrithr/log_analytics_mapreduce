Analytics on top of Apache http web logs:

Classes Description: (common package: `com.cloudwick.mapreduce.loganalytics`)

| Class Name | Description |
| ---------- | ----------- |
| *logsizeaggregator.DriverLogSizeAggregator* | Aggregates the web log messages size by max, min, mean |
| *hitsperurl.DriverHitsPerUrl* | Aggregates number of times a url has been visited |
| *hitsperhour.DriverHitsPerHour* | Aggregates the number of hits received per hour |
| *msgsizehits.DriverMsgSizeVsHits* | Analyzes the data to find the relationship between the size of the web pages and the number of hits received by the web page |
| *statuscounter.DriverStatusCounter* | Counts the number of times a status code has returned by webserver |
| *custom.Driver* | Illustrates the use of custom FileFormat, RecordReader, Partitioner, WritableComparable |


To build jar:

```
mvn package
```

To run a specific class (ex: logsizeaggregator.DriverLogSizeAggregator):

```
hadoop jar loganalytics-1.0*.jar com.cloudwick.mapreduce.loganalytics.logsizeaggregator.DriverLogSizeAggregator [input_path] [output_path]
```

To generate synthetic data use [generater](http://github.com/cloudwicklabs/generator)

**License and Authors**

Authors: [Ashrith](http://github.com/ashrithr)

Apache 2.0. Please see LICENSE.txt. All contents copyright (c) 2013, Ashrith.

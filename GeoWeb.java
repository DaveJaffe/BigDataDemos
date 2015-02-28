/*
GeoWeb.java: GeoWeb application driver
 
Last Updated 10/17/13

Dave Jaffe

Distributed under Creative Commons with Attribution by Dave Jaffe (davejaffe7@gmail.com).  
Provided as-is without any warranties or conditions.

Analyzes Apache web logs 
* Determines remote location from IP address
* Reports number of clicks per country per hour

Syntax:
hadoop jar GeoWeb.jar GeoWeb -files all_classbs.txt <HDFS Apache web log directory> <HDFS output directory>
  
Input: Apache web logs
Output: Number of clicks in logs per country per hour
  
File all_classbs.txt must exist in local directory
all_classbs.txt: a list of class B IP addresses for all countries from the GeoLite dataset
  Only those class B's that come from a single country are used. 
  Example line: 23.242 US

This product includes GeoLite data created by MaxMind, available from http://www.maxmind.com

Example weblog line:
172.16.3.1 - - [27/Jun/2012:17:48:34 -0500] "GET /favicon.ico HTTP/1.1" 404 298 "-" "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)"

Remote host/-/-/Date-timestamp/Request line/status/size/-/Referer/User agent

See http://httpd.apache.org/docs/current/logs.html
*/

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeoWeb extends Configured implements Tool
  {
  @Override
  public int run(String[] args) throws Exception
    {
    if (args.length != 2)
      {
      System.out.println("Usage: GeoWeb <input dir> <output dir>\n");
      return -1;
      }

    Job job = new Job(getConf());
    job.setJarByClass(GeoWeb.class);
    job.setJobName("GeoWeb");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(GeoWebMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setCombinerClass(SumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
    }

  public static void main(String[] args) throws Exception
    {
    int exitCode = ToolRunner.run(new Configuration(), new GeoWeb(), args);
    System.exit(exitCode);
    }
  } // End Class GeoWeb

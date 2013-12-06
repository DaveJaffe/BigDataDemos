/*
CreateWeblogs.java: CreateWeblogs application driver
 
Last Updated 10/29/13

Dave Jaffe, Dell Solution Centers

Distributed under Creative Commons with Attribution by Dave Jaffe (dave_jaffe@dell.com).  
Provided as-is without any warranties or conditions.

Creates Apache web logs using map-only program, CreateWeblogsMapper.java
* Uses real distribution of remote ips per country based on GeoLite data from MaxMind for top 20 web-using countries
* Assumes consumer oriented website located in Central time zone - more activity in evening local time
* Does simple sessionization (user clickstreams are sequential, don't overlap days)

Syntax:
hadoop jar CreateWeblogs.jar CreateWeblogs -files all_classbs.txt,referrers.txt,requests.txt,user_agents.txt \
  <HDFS input directory> <HDFS output directory>
  
Input: file(s) containing lines with space-separated year, month, day, number of records per day
Output: weblogs with name access_log-yyyymmdd 
  
Files all_classbs.txt, referrers.txt, requests.txt, user_agents.txt must exist in local directory
all_classbs.txt: a list of class B IP addresses and associated country, from the GeoLite dataset
  Only those class B's that come from a single country are used. Full IP addresses are randomly generated from these
  class B addresses following the country distribution of Internet use.
  Example line: 23.242 US
requests.txt: a list of the request part of the web log. These are randomly added to the web log.
  Example line: GET /ds2/dslogin.php?username=user15363&password=password HTTP/1.1
referrers.txt: a list of the referrers part of the web log. These are randomly added to the web log.
  Example line: http://110.240.0.17/
user_agents.txt: a list of the user_agents part of the web log. These are randomly added to the web log.
  Example line: Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; .NET CLR 3.0.04506;)

This product includes GeoLite data created by MaxMind, available from http://www.maxmind.com

Example weblog line:
172.16.3.1 - - [27/Jun/2012:17:48:34 -0500] "GET /favicon.ico HTTP/1.1" 404 298 "-" "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)"

Remote host/-/-/Date-timestamp/Request line/status/size/-/Referer/User agent

See http://httpd.apache.org/docs/current/logs.html
*/

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CreateWeblogs extends Configured implements Tool
  {
  @Override
  public int run(String[] args) throws Exception
    {
    if (args.length != 2)
      {
      System.out.print("Usage: CreateWeblogs <input dir> <output dir>\n");
      return -1;
      }

    Job job = new Job(getConf());
    job.setJarByClass(CreateWeblogs.class);
    job.setJobName("CreateWeblogs");
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(CreateWeblogsMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
    }

  public static void main(String[] args) throws Exception
    {
    int exitCode = ToolRunner.run(new Configuration(), new CreateWeblogs(), args);
    System.exit(exitCode);
    }
  } // End Class CreateWeblogs


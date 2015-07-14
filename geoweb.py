"""
geoweb.py: Spark program to analyze Apache web logs, counting hits by country and hour

Last Updated 7/13/15

Distributed under Creative Commons with Attribution by Dave Jaffe (davejaffe7@gmail.com)
Provided as-is without any warranties or conditions

Analyzes Apache web logs
* Determines remote host from IP address
* Reports number of clicks per country per hour

To run: spark-submit geoweb.py <access log directory> all_classbs.txt result_file
  where <access log directory> is local or HDFS filesystem directory containing Apache web server access logs
  and   all_classbs.txt is a list of class B IP addresses for all countries from the GeoLite dataset
    Only those class B's that come from a single country are used.
    Example line: 23.242 US
  and result_file is file with number of clicks in logs per country per hour

Sample invocations:
local filesystem:
spark-submit geoweb.py access_logs all_classbs.txt spark_output
HDFS:
spark-submit geoweb.py hdfs://namenode.example.com:8020/user/test/weblogs/access_logs hdfs://namenode.example.com:8020/user/test/weblogs/classbs/all_classbs.txt spark_output

This product includes GeoLite data created by MaxMind, available from http://www.maxmind.com

Apache web log format:
Remote host/-/-/Date-timestamp/Request line/status/size/-/Referer/User agent
Example weblog line:
172.16.3.1 - - [27/Jun/2012:17:48:34 -0500] "GET /favicon.ico HTTP/1.1" 404 298 "http://www.example.com/start.html" "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)"
"""

#Apache web log parse program based on code from BerkeleyX: CS100.1x Introduction to Big Data with Apache Spark course
def parseApacheLogLine(logline):
  """ Parse a line in the Apache Common Log format
  Args:
      logline (str): a line of text in the Apache Common Log format
  Returns:
      tuple: either a dictionary containing the parts of the Apache Access Log and 1, or the original invalid log line and 0
  """
  import re
  APACHE_ACCESS_LOG_PATTERN = '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] "(\\S+) (\\S+)\\s*(\\S*)\\s*" (\\d{3}) (\\S+)\\s*("[^"]*")?\\s*("[^"]*")?'
  match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
  if match is None:
      return (logline, 0)
  size_field = match.group(9)
  if size_field == '-':
      size = long(0)
  else:
      size = long(match.group(9))
  return (
      {
      'host'          : match.group(1),
      'client_identd' : match.group(2),
      'user_id'       : match.group(3),
      'date_time'     : parse_apache_time(match.group(4)),
      'timezone'      : match.group(4)[21:26],
      'method'        : match.group(5),
      'endpoint'      : match.group(6),
      'protocol'      : match.group(7),
      'response_code' : int(match.group(8)),
      'content_size'  : size,
      'referer'       : match.group(10),
      'user_agent'    : match.group(11)
      }
      , 1)

def parse_apache_time(s):
  """ Convert Apache time format into a Python datetime object
  Args:
      s (str): date and time in Apache time format
  Returns:
      datetime: datetime object (ignore timezone for now)
  """
  import datetime
  month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}
  return datetime.datetime(int(s[7:11]), month_map[s[3:6]], int(s[0:2]), int(s[12:14]), int(s[15:17]), int(s[18:20]))

def parseLogs(sc, logFile):
  """ Read and parse log file """
  parsed_logs = sc.textFile(logFile).map(parseApacheLogLine).cache()
  access_logs = parsed_logs.filter(lambda s: s[1] == 1).map(lambda s: s[0]).cache()
  failed_logs = parsed_logs.filter(lambda s: s[1] == 0).map(lambda s: s[0])
  print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines\n' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
  return access_logs

# geoweb.py main program starts here

import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("geoweb").set("spark.ui.port", "4041")
sc = SparkContext(conf = conf)
if len(sys.argv) != 4:
  print "Syntax: spark-submit geoweb.py <access log directory> all_classbs.txt result_file"
  sys.exit()

# Parse access logs from all files in input directory, store in access_logs RDD
access_logs = parseLogs(sc, sys.argv[1])

# Pull remote hostname, hour of web access from access logs
host_hour = access_logs.map((lambda log: (log['host'], log['date_time'].hour)))

# Split host by periods, creating a list of the four octets or hostname parts, filter for numeric IP address (TODO: DNS lookup for non-numeric hostnames)
o4_hour = host_hour.map(lambda (host, hour): (host.split('.'), hour)).filter(lambda (o4,hour): o4[0].isnumeric() and o4[1].isnumeric())

# Take first 2 octets along with hour: o2_hour is RDD of tuples like (u'114.164', 0)
o2_hour = o4_hour.map(lambda (o4,hour): (o4[0]+'.'+o4[1], hour))

# Read all_classbs.txt file, create RDD from it. acb is RDD of lists like [u'1.3', u'CN']
acb = sc.textFile(sys.argv[2]).map(lambda line: line.split(' '))

# Join class B's with o2_hour RDD along first 2 octets; pull out country+hour tuples; count total in country+hour; sort
country_hour = acb.join(o2_hour).map(lambda (k,v): (v,1)).reduceByKey(lambda a, b : a + b).sortByKey().collect()

# Print in form: country+hour(tab)count, eg. US20	47
result_file = open(sys.argv[3], "w")
for ch in country_hour:
  result_file.write("%s%02d\t%d\n" % (ch[0][0], ch[0][1], ch[1]))
result_file.close()

-- geoweb.q: Hive commands to analyze Apache web logs for location and hour
-- Last Updated 11/1/13
-- Dave Jaffe, Dell Solution Centers
-- Distributed under Creative Commons with Attribution by Dave Jaffe 
-- (dave_jaffe@dell.com).  Provided as-is without any warranties or conditions.

ADD JAR /usr/lib/hive/lib/hive-contrib-0.9.0-Intel.jar; 

-- specify number of reduce tasks
SET mapred.reduce.tasks=200; 

-- read web logs with Regex serde
DROP TABLE weblogs;
CREATE EXTERNAL TABLE weblogs (
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size STRING,
  referer STRING,
  agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\"))?",
  "output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s"
)
stored as textfile
-- Location of access logs to be analyzed
location '/user/test/weblogs/access_logs_1TB';

--read file containing all class B IP addresses and country (eg: 184.78 US)
DROP TABLE classbs;
CREATE EXTERNAL TABLE classbs (
  IP_B STRING,
  ctry_code STRING)
row format delimited
fields terminated by ' '
stored as textfile
location '/user/test/weblogs/classbs';

-- Create temporary table with first two octets of host IP and hour 
DROP TABLE t2; 
CREATE TABLE t2 AS SELECT REGEXP_EXTRACT(host,"^([0-9]{1,3})\.([0-9]{1,3})",0) AS host_ip_b, SUBSTRING(time,14,2) AS hour FROM weblogs;
-- Join temporary table to list of Class B addresses
SELECT classbs.ctry_code, t2.hour, COUNT(*) FROM t2 JOIN classbs ON (t2.host_ip_b = classbs.ip_b) GROUP BY classbs.ctry_code, t2.hour;

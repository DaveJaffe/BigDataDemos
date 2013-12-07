-- geoweb.pig: Pig script to analyze Apache web logs, counting hits by country and hour
-- Last Updated 11/1/13
-- Dave Jaffe, Dell Solution Centers
-- Distributed under Creative Commons with Attribution by Dave Jaffe 
-- (dave_jaffe@dell.com).  Provided as-is without any warranties or conditions.

REGISTER /usr/lib/pig/lib/piggybank.jar

-- Load apache web logs from HDFS
weblog = LOAD '/user/root/weblogs/access_logs_1TB/*' USING org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader AS (remoteAddr, remoteLogname, user, time, method, uri, proto, status, bytes, referer, userAgent);

-- Load file of Class B IP addresses - each line consists of IP address and ctry, eg.  184.78 US
classbs = LOAD '/user/root/weblogs/classbs/all_classbs.txt' USING PigStorage(' ') AS (ip_b:chararray, ctry_code:chararray);

-- Pull first two octets of IP address and hour from web log
A = FOREACH weblog GENERATE REGEX_EXTRACT((chararray)remoteAddr,'^([0-9]{1,3})\\.([0-9]{1,3})',0) AS host_ip_b, SUBSTRING((chararray) time,12,14) AS hour;
-- Join with table of IP addresses, group by country code and hour, order and store result back to HDFS
B = JOIN A by host_ip_b, classbs by ip_b PARALLEL 200;
C = GROUP B BY (ctry_code, hour) PARALLEL 200;
D = FOREACH C GENERATE FLATTEN($0), COUNT($1) as count;
E = ORDER D BY ctry_code, hour PARALLEL 200;
STORE E into '/user/root/weblogs/w1TB_pig_out2' USING PigStorage;

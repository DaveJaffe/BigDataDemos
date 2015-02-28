-- geoweb.pig: Pig script to analyze Apache web logs, counting hits by country and hour
-- Last Updated 11/1/13
-- Distributed under Creative Commons with Attribution by Dave Jaffe (davejaffe7@gmail.com)
-- Provided as-is without any warranties or conditions

-- Change if necessary
REGISTER /usr/lib/pig/lib/piggybank.jar

-- Load apache web logs from HDFS
weblog = LOAD '/user/test/weblogs/access_logs/*' USING org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader AS (remoteAddr, remoteLogname, user, time, method, uri, proto, status, bytes, referer, userAgent);

-- Load file of Class B IP addresses - each line consists of IP address and ctry, eg.  184.78 US
classbs = LOAD '/user/test/weblogs/classbs/all_classbs.txt' USING PigStorage(' ') AS (ip_b:chararray, ctry_code:chararray);

-- Pull first two octets of IP address and hour from web log
A = FOREACH weblog GENERATE REGEX_EXTRACT((chararray)remoteAddr,'^([0-9]{1,3})\\.([0-9]{1,3})',0) AS host_ip_b, SUBSTRING((chararray) time,12,14) AS hour;
-- Join with table of IP addresses, group by country code and hour, order and store result back to HDFS
B = JOIN A by host_ip_b, classbs by ip_b PARALLEL 20;
C = GROUP B BY (ctry_code, hour) PARALLEL 20;
D = FOREACH C GENERATE FLATTEN($0), COUNT($1) as count;
E = ORDER D BY ctry_code, hour PARALLEL 20;
STORE E into '/user/test/weblogs/w_pig_out' USING PigStorage;

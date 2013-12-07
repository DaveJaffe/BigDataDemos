GeoWeb - Programs for Analyzing Apache Web Logs
===============================================

See whitepaper, Three Approaches to Data Analysis with Hadoop, at http://en.community.dell.com/techcenter/extras/m/white_papers/20437941/download.aspx

MapReduce, Hive and Pig programs to analyze Apache web logs
- parses remote IP address, hour from web log
- uses file all_classbs.txt to map remote IP address to country of origin
- sums up hits per hour per country across all input web logs

MapReduce:
- Files: GeoWeb.java, GeoWebMapper.java, SumReducer.java, all_classbs.txt
- Compile code into hadoop jar file GeoWeb.jar
- Example invocation:
hadoop jar GeoWeb.jar GeoWeb -files all_classbs.txt -D  mapred.reduce.tasks=200 /user/test/weblogs/access_logs_1TB /user/test/weblogs/w1TB_mr_out
- Collect and sort output:
hadoop fs -cat /user/test/weblogs/w1TB_mr_out/part* | sort > r_1TB_mr

Hive:
- Files: geoweb.q, all_classbs.txt
- Modify geoweb.q to reflect location of input web logs, all_classbs.txt file
- Example invocation:
hive –f geoweb.q > hive1TB.out
- To put in same form as MapReduce output:
sed 's/\t//' hive1TB.out | sort > r_1TB_hive

Pig:
- Files: geoweb.pig, all_classbs.txt
- Modify geoweb.pig to reflect location of input web logs, all_classbs.txt file, HDFS output location
- Example invocation:
pig geoweb.pig
- Collect and sort output:
hadoop fs -cat /user/test/weblogs/w1TB_pig_out/part* | sed 's/\t//' | sort > r_1TB_pig


All 3 results files should be identical for same input logs

GeoWeb - Programs for Analyzing Apache Web Logs
===============================================

MapReduce, Hive and Pig programs to analyze Apache web logs
- parses remote IP address, hour from web log
- uses file all_classbs.txt to map remote IP address to country of origin
- sums up hits per hour per country across all input web logs

MapReduce:
- Files: GeoWeb.java, GeoWebMapper.java, SumReducer.java, all_classbs.txt
- Compile code into hadoop jar file GeoWeb.jar
- Example invocation:
hadoop jar GeoWeb.jar GeoWeb -files all_classbs.txt -D  mapred.reduce.tasks=200 /user/test/weblogs/access_logs_1TB /user/test/weblogs/w1TB_mr_out

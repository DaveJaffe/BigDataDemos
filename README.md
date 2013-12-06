BigDataDemos
============

Demo programs for Hadoop etc.

Dave Jaffe 
dave_jaffe@dell.com


GeoWeb Apache Log Generator
===========================

The GeoWeb Apache Log Generator generates synthetic yet realistic Apache web logs with specified geographic and temporal distributions of web accesses. The program produces sequential web logs for a specified month, day, year and number of web accesses per day. Sessioning is simulated with the average number of clicks per user configurable in the code.

The program uses a table matching IP addresses to countries derived from GeoLite data from MaxMind (http://www.maxmind.com) to generate geographically realistic remote IP addresses in the logs.  The remote IP addresses generated by the program are distributed geographically among the top 20 Internet-using countries according to their number of users. The distribution can be readily changed in the code if desired.

The Log Generator models a consumer or social web site operating in the US Central time zone so that usage from each country peaks during their local evening hours. For simplicity a single time zone is assigned to each country. 

The web request (such as a GET command), referrer (referring web site if present), and user agent (web browser used) fields are randomly selected from input files containing lists of each item. The user can thus tailor the weblogs to their particular application. In the current code these fields are randomized but with some development the program could be used to generate meaningful log sequences for clickstream analysis.

Implemented as a Map-only MapReduce program, the program reads input files with lines containing the year, month, day and number of web accesses per day, and creates a file in the specified output HDFS directory with the name access_logs-yyyymmdd, containing that number of web accesses.

As an example, if the input files contain 366 lines of the form
yyyy mm dd 11900000
then 366 web log files totaling 1TB will be created. The number of map tasks employed will be equal to the number of input files.

Because it is a MapReduce program the Log Generator is extremely scalable. A 1TB set of web logs was generated in 10 minutes on a 20-datanode cluster.

Compile the program as a standard Hadoop jar file. To run the program, create the input files and copy to an HFDS directory, then place the Class B IP address file and the referrer, request and user agent files in the local directory and:

hadoop jar CreateWeblogs.jar CreateWeblogs –files all_classbs.txt,referrers.txt,requests.txt,user_agents.txt \<HDFS input directory> \<HDFS output directory>

Files: CreateWeblogs.java, CreateWeblogsMapper.java, all_classbs.txt, referrers.txt, requests.txt, user_agents.txt

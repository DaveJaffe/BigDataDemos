BigDataDemos
============

Demo programs for Hadoop etc.

Dave Jaffe 
davejaffe7@gmail.com
@davejaffe7


GeoWeb Apache Log Generator and Analysis Tools
==============================================

Demo programs to generate Apache web logs and analyze them with MapReduce, Hive and Pig

See whitepaper, Three Approaches to Data Analysis with Hadoop, at http://en.community.dell.com/techcenter/extras/m/white_papers/20437941/download.aspx

GeoWeb Apache Log Generator 
- MapReduce program to generate large volumes of realistic Apache web logs
- Files: CreateWeblogs.java, CreateWeblogsMapper.java, all_classbs.txt, referrers.txt, requests.txt, user_agents.txt
- See GeoWebApacheWebLogGenerator_readme.md

GeoWeb MapReduce Program
- MapReduce program to analyze Apache web logs, producing counts per country per hour
- Files: GeoWeb.java, GeoWebMapper.java, SumReducer.java, all_classbs.txt
- See GeoWeb_readme.md

GeoWeb Hive Script
- Hive script to analyze Apache web logs, producing counts per country per hour
- Files: geoweb.q, all_classbs.txt
- See GeoWeb_readme.md

GeoWeb Pig Script
- Pig script to analyze Apache web logs, producing counts per country per hour
- Files: geoweb.pig, all_classbs.txt
- See GeoWeb_readme.md

#!/bin/bash

  echo "************* Start Processing ********************"

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

$HBASE_HOME/bin/start-hbase.sh


echo   "/usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/RatingFlume.conf -Dflume.root.looger=DEBUG,console -Xmx2g"
       /usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/RatingFlume.conf -Dflume.root.looger=DEBUG,console -Xmx2g &
echo " PID=`ps -ef | grep usr/hadoop/Flume/conf/RatingFlume.conf: | grep -v 'grep' | awk '{print $2}'`" 
        PID=`ps -ef | grep usr/hadoop/Flume/conf/RatingFlume.conf: | grep -v 'grep' | awk '{print $2}'`
        sleep 9m
         kill -TERM  ${PID}

echo "/usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/MoviesFlume.conf -Dflume.root.looger=DEBUG,console -Xmx2g"
       /usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/MoviesFlume.conf -Dflume.root.looger=DEBUG,console -Xmx2g &
         PID=`ps -ef | grep usr/hadoop/Flume/conf/MoviesFlume.conf: | grep -v 'grep' | awk '{print $2}'`
        sleep 1m
         kill -TERM  ${PID}


 echo "hadoop jar /data/hadoopQuery.jar query.Query1 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie hdfs:///OutputQuery1 > resultQuery1.txt"
       hadoop jar /data/hadoopQuery.jar query.Query1 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie hdfs:///OutputQuery1 > resultQuery1.txt

echo "hadoop jar /data/hadoopQuery.jar query.Query2 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie  hdfs:///OutputQuery2 > resultQuery2.txt " 
      hadoop jar /data/hadoopQuery.jar query.Query2 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie  hdfs:///OutputQuery2 > resultQuery2.txt

echo "hadoop jar /data/hadoopQuery.jar query.Query3step4 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie  hdfs:///OutputQuery3 > resultQuery3.txt"
      hadoop jar /data/hadoopQuery.jar query.Query3step4 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie  hdfs:///OutputQuery3 > resultQuery3.txt


  echo "************* End Processing  ********************"


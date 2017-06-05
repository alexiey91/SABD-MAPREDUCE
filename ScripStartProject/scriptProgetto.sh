#!/bin/bash
9
  echo "************* Start Processing ********************"
   
#  echo "PID=`ps -ef | grep usr/local/hadoop/Flume/conf/testFlumePaolo.conf: | grep -v 'grep' | awk '{print $2}'`"
#	PID=`ps -ef | grep usr/hadoop/Flume/conf/testFlumePaolo.conf: | grep -v 'grep' | awk '{print $2}'`

     
echo   "/usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/testFlumePaolo.conf -Dflume.root.looger=DEBUG,console -Xmx2g"
       /usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/testFlumePaolo.conf -Dflume.root.looger=DEBUG,console -Xmx2g &
echo " PID=`ps -ef | grep usr/hadoop/Flume/conf/testFlumePaolo.conf: | grep -v 'grep' | awk '{print $2}'`"	
	PID=`ps -ef | grep usr/hadoop/Flume/conf/testFlumePaolo.conf: | grep -v 'grep' | awk '{print $2}'` 
	sleep 10m 
	 kill -TERM  ${PID} 

echo "/usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/MoviesFlume.conf -Dflume.root.looger=DEBUG,console -Xmx2g"
       /usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/MoviesFlume.conf -Dflume.root.looger=DEBUG,console -Xmx2g &
	 PID=`ps -ef | grep usr/hadoop/Flume/conf/MoviesFlume.conf: | grep -v 'grep' | awk '{print $2}'`
        sleep 10m
         kill -TERM  ${PID}
 

 echo "hadoop jar /data/mapreducedesignpatterns.jar query.Query1 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie hdfs:///OutputQuery1"
       hadoop jar /data/mapreducedesignpatterns.jar query.Query1 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie hdfs:///OutputQuery1

echo "hadoop jar /data/mapreducedesignpatterns.jar query.Query2 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie  hdfs:///OutputQuery2"
      hadoop jar /data/mapreducedesignpatterns.jar query.Query2 hdfs:///SpoolRating/usr/local/hadoop/Flume/SpoolDir hdfs:///SpoolMovies/usr/local/hadoop/Flume/SpoolMovie  hdfs:///OutputQuery2

  echo "************* End Processing  ********************"


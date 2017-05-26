#!/bin/bash
FILES=/usr/local/hadoop/ingest/*.csv
for f in $FILES
do
filename=`basename $f`
name=`echo $f | cut -f1 -d'.'`
  echo "************* Processing $name ********************"
echo "./kite-dataset csv-schema $name.csv --class `basename $name` -o $name.avsc"
       ./kite-dataset csv-schema $name.csv --class `basename $name` -o $name.avsc
echo "./kite-dataset create  dataset:hdfs:/Kite/`basename $name` --schema $name.avsc "
       ./kite-dataset create  dataset:hdfs:/Kite/`basename $name` --schema $name.avsc 
echo "./kite-dataset -v csv-import $name.csv --delimiter ','   dataset:hdfs:/Kite/`basename $name`"
       ./kite-dataset -v csv-import $name.csv --delimiter ',' dataset:hdfs:/Kite/`basename $name`
  echo "************* Processing $name ********************"
done

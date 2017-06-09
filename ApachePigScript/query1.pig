/* Import csv from Hdfs to Pig Storage Movies e Rating*/



rating = LOAD 'hdfs://master:54310/Ratings' USING PigStorage(',') as
                (userid:int, movieid:int, rating:double, timestamp:int );


movies = LOAD 'hdfs://master:54310/Movies' USING PigStorage(',') as
                (movieid:int , title:chararray, genres: chararray);



filter_rating = FILTER rating BY  timestamp >= 946681200;



filter_average = GROUP filter_rating BY movieid;



filter_average_rating = FOREACH filter_average  GENERATE flatten(group) as (movieid) , AVG(filter_rating.rating) as (rating), COUNT(filter_rating.rating) as (numero_votanti);

filter_votanti = FILTER filter_average_rating BY numero_votanti > 5;

filter_prejoin = FILTER filter_votanti BY rating >= 4;



join_table  = JOIN movies BY movieid, filter_prejoin BY movieid;



query1 = FOREACH join_table GENERATE title,rating;



--result = ILLUSTRATE query1;

--Illustrate query1;
Dump query1;
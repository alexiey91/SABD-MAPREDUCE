rating =LOAD 'hdfs://master:54310/ratingT.csv' USING PigStorage(',') as
                (userid:int, movieid:int, rating:double, timestamp:int);


movies = LOAD 'hdfs://master:54310/movieT.csv' USING PigStorage(',') as
                (movieid:int , title:chararray, genres: chararray);


filter_rating_2013_2014 = FILTER rating BY  (timestamp >= 1364767200) AND (timestamp <= 1396216800) ;

filter_rating_2014_2015 = FILTER rating BY (timestamp >= 1396303200) AND (timestamp <=1427752800);


filter_group_rating_2013_2014 = GROUP filter_rating_2013_2014 BY movieid;

filter_group_rating_2014_2015 = GROUP filter_rating_2014_2015 BY movieid;




filter_average_rating_2013_2014 = FOREACH filter_group_rating_2013_2014  GENERATE flatten(group) as (movieid) , AVG(filter_rating_2013_2014.rating) as (rating), COUNT(filter_rating_2013_2014.rating) as (numero_votanti);

filter_average_rating_2014_2015 = FOREACH filter_group_rating_2014_2015  GENERATE flatten(group) as (movieid) , AVG(filter_rating_2014_2015.rating) as (rating), COUNT(filter_rating_2014_2015.rating) as (numero_votanti);


filter_votanti_2013_2014 = FILTER filter_average_rating_2013_2014 BY numero_votanti > 15;

filter_votanti_2014_2015 = FILTER filter_average_rating_2014_2015 BY numero_votanti > 15;

--floor

FLOOR_2013_2014 = FOREACH filter_votanti_2013_2014 GENERATE ROUND_TO(rating,1) as (rating), movieid;

FLOOR_2014_2015 = FOREACH filter_votanti_2014_2015 GENERATE ROUND_TO(rating,1) as (AVGrating) , movieid as (filmid15) ;


--ordini
order_rating_2013_2014 = ORDER FLOOR_2013_2014 BY rating DESC;

order_rating_2014_2015 = ORDER FLOOR_2014_2015 BY AVGrating DESC;

--Posizioni 2013_2014
ranked_2013_2014 = rank order_rating_2013_2014 ;

--Top Ten

top_ten_2014_2015 = LIMIT order_rating_2014_2015 10;
ranked_2014_2015 = rank top_ten_2014_2015;

--join tabelle rating

join_total = JOIN ranked_2013_2014 BY movieid RIGHT, ranked_2014_2015 BY filmid15;


rating_total_position = FOREACH join_total GENERATE (filmid15) as (movieid), (rank_top_ten_2014_2015) as (currentPosition) , (rank_order_rating_2013_2014-rank_top_ten_2014_2015) as (difference), (AVGrating) as (rating);

--join finale
last_join = JOIN movies BY movieid, rating_total_position BY movieid;

--result
query3 = FOREACH last_join GENERATE title,rating,currentPosition,difference;


Dump query3;


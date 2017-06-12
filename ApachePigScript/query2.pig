REGISTER  /usr/lib/pig/datafu-pig-incubating-1.3.1.jar;

define VAR datafu.pig.stats.VAR();

rating = LOAD 'hdfs://master:54310/ratingT.csv' USING org.apache.pig.piggybank.storage.CSVLoader()  as
                (userid:int, movieid:int, rating:double, timestamp:int);


movies = LOAD 'hdfs://master:54310/movieT.csv' USING org.apache.pig.piggybank.storage.CSVLoader()  as
                (movieid:int , title:chararray, genres: chararray);



group_rating = GROUP rating BY movieid;

new_rating = FOREACH group_rating GENERATE FLATTEN(group) as (filmid), AVG(rating.rating) as rating , VAR (rating.rating) as variance;

join_all = JOIN movies BY movieid, new_rating BY filmid;



join_all_2 = FOREACH join_all GENERATE   movieid,genres, rating, variance;

strsplit_genres = FOREACH join_all_2  GENERATE FLATTEN(STRSPLITTOBAG (genres,'\\|',19)) as newGenres,rating,variance;

group_genres = GROUP strsplit_genres BY newGenres;

avg_genres = FOREACH group_genres GENERATE FLATTEN(group) as newGenres ,AVG(strsplit_genres.rating) as average, AVG(strsplit_genres.variance) as variance;

--Dump group_genres;



action = FILTER avg_genres BY (newGenres matches 'Action');
adventure = FILTER avg_genres BY (newGenres matches 'Adventure');
animation = FILTER avg_genres BY (newGenres matches 'Animation');
children = FILTER avg_genres BY (newGenres matches 'Children');
comedy = FILTER avg_genres BY (newGenres matches 'Comedy');
crime = FILTER avg_genres BY (newGenres matches 'Crime');
documentary = FILTER avg_genres BY (newGenres matches 'Documentary');
drama = FILTER avg_genres BY (newGenres matches 'Drama');
fantasy = FILTER avg_genres BY (newGenres matches 'Fantasy');
filmnoir = FILTER avg_genres BY (newGenres matches 'Film-Noir');
horror = FILTER avg_genres BY (newGenres matches 'Horror');
imax = FILTER avg_genres BY (newGenres matches 'IMAX');
musical = FILTER avg_genres BY (newGenres matches 'Musical');
mystery = FILTER avg_genres BY (newGenres matches 'Mystery');
romance = FILTER avg_genres BY (newGenres matches 'Romance');
scifi = FILTER avg_genres BY (newGenres matches 'Sci-Fi');
thriller = FILTER avg_genres BY (newGenres matches 'Thriller');
war = FILTER avg_genres BY (newGenres matches 'War');
western = FILTER avg_genres BY (newGenres matches 'Western');
no_genres = FILTER avg_genres BY (newGenres matches '\\(no genres listed\\)');

final = UNION action, adventure, animation, children, comedy, crime, documentary, drama, fantasy, filmnoir, horror,imax, musical, mystery, romance, scifi, thriller, war, western ,no_genres;


STORE final INTO 'hdfs://master:54310/Result';

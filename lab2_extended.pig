A = LOAD '/home/mkobbi/workspace/mkobbi_-_m2_dk_mdm_pig/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);
movies = LOAD '/home/mkobbi/workspace/mkobbi_-_m2_dk_mdm_pig/ml-20m/movies.csv' USING PigStorage(',') AS (movieId:int, title:chararray, genres:chararray) ;
tags =  LOAD '/home/mkobbi/workspace/mkobbi_-_m2_dk_mdm_pig/ml-20m/tags.csv' USING PigStorage(',') AS (userId:int, movieId:int, tag:chararray, timestamp:long);

B = FILTER A BY rating>0.5 AND rating<5.0;
C = GROUP B BY userId;
D = FOREACH C GENERATE group AS userId, AVG(B.rating) AS avgRating;

grouped_by_user = GROUP A BY userId;
count_by_user =  FOREACH grouped_by_user GENERATE group AS userId, COUNT_STAR(A.userId) as count;
filtered_by_count = FILTER count_by_user BY count>=100;
/*DUMP count_by_user;*/
EXPLAIN filtered_by_count;

grouped_by_movie = GROUP B BY movieId;
illustrate grouped_by_movie;
count_by_movie = FOREACH grouped_by_movie GENERATE group AS movieId, COUNT_STAR(B.movieId) as count;
/*DUMP count_by_movie;*/
EXPLAIN count_by_movie;

grouped_by_movie = GROUP B BY movieId;
average_by_movie = FOREACH grouped_by_movie GENERATE group AS movieId, AVG(B.rating) AS avgRating;
joined_by_movie = JOIN average_by_movie by movieId, movies by movieId;
filtered_by_genre = FILTER joined_by_movie BY ( movies::genres matches '.*Documentary.*' ) ;
cleansed_by_genre = FOREACH filtered_by_genre GENERATE $3 AS title, $1 as average_rating;
/*DUMP cleansed_by_genre;*/
EXPLAIN cleansed_by_genre;

movie_tags_all = GROUP tags BY movieId;

movie_tags_distinct = FOREACH movie_tags_all { 
    unique_segments = DISTINCT tags.tag;
    GENERATE group AS movieId, COUNT_STAR(unique_segments) as tags_count;
};
tag_movie_join = JOIN movie_tags_distinct by movieId, movies by movieId;
/*DUMP tag_movie_join;*/
filtered_by_action = FILTER tag_movie_join BY ( $4 matches '.*Action.*' );
cleansed_by_action = FOREACH filtered_by_action GENERATE $3 AS title, $1 as count;
/*dump cleansed_by_action;*/
EXPLAIN cleansed_by_action;



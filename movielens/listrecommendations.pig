movies = load '/raw/movielens/movie' AS (movieid, name, year);
recs = load 'recs' AS (userid, reclist);

longlist = FOREACH recs GENERATE 
    userid, FLATTEN(TOKENIZE(reclist)) AS movieandscore;

finallist = FOREACH longlist GENERATE userid, REGEX_EXTRACT(movieandscore, '(\\d+)', 1) AS movieid;

results = JOIN finallist BY movieid, 
                  movies BY movieid;

final = FOREACH results GENERATE userid, name;
srtd = ORDER final BY userid;

DUMP srtd;
CREATE DATABASE final_project;
CREATE TABLE ehrData (business_state_territory STRING, num_products INT, largest_provider STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA INPATH '/final-project-output-ehrData/part-00000'
OVERWRITE INTO TABLE ehrData;
CREATE TABLE stateTable (business_state_territory STRING, region STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA INPATH '/final-project-output-stateTable/part-00000'
OVERWRITE INTO TABLE stateTable;
SELECT e.*, s.region FROM ehrData e LEFT OUTER JOIN stateTable s
ON (e.business_state_territory = s.business_state_territory)
WHERE (s.region != "commonwealth" and s.region != "territory" and s.region != "minor")
ORDER BY num_products
LIMIT 5;






SELECT e.*, s.region FROM ehrData e LEFT OUTER JOIN stateTable s
ON (e.business_state_territory = s.business_state_territory)
ORDER BY num_products
LIMIT 5;


SELECT * FROM stateTable
WHERE (region != "commonwealth" and region != "territory" and region != "minor");



SELECT i.movieID, i.movie_title, i.release_date, i.imdb_url, CAST(AVG(r.rating) AS DECIMAL(10,1)) FROM item i FULL OUTER JOIN rating r
ON (i.movieID = r.itemID)
WHERE (i.movieID = 376 or i.movieID = 495)
GROUP BY i.movieID, i.movie_title, i.release_date, i.imdb_url;

SELECT * FROM ehrData
ORDER BY num_products
LIMIT 20;
#Create a database to hold the EHR and United States regional tables.

CREATE DATABASE final_project;

#Create the EHR and United States regional tables.

CREATE TABLE ehrData (business_state_territory STRING, num_products INT, largest_provider STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE stateTable (business_state_territory STRING, region STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

#Load in the data from the Spark applications

LOAD DATA INPATH '/final-project-output-ehrData/part-00000'
OVERWRITE INTO TABLE ehrData;

LOAD DATA INPATH '/final-project-output-stateTable/part-00000'
OVERWRITE INTO TABLE stateTable;

#Join the two tables and query the results for regions within the United States that have the lowest number of EHR products.

SELECT e.*, s.region FROM ehrData e LEFT OUTER JOIN stateTable s
ON (e.business_state_territory = s.business_state_territory)
WHERE (s.region != "commonwealth" and s.region != "territory" and s.region != "minor")
ORDER BY num_products
LIMIT 5;
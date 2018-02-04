Overview
-------------------

The following program can be used by a company entering the Electoric Health Care (EHR) provider market to determine which state, region, and provider type to target with a marketing campaign. The datasets used in this project can be obtained from the following databases.

ehrData: https://dashboard.healthit.gov/datadashboard/documentation/ehr-products-mu-attestation-data-documentation.php
stateTAble: https://statetable.com/

Dependicies
-------------------

Hadoop = 2.8.1
Spark = 1.6.3 Without Hadoop
Scala = 2.10.6
SBT = 0.13.16
Hive = 1.2.2

Step-by-Step Instructions
-------------------

Make a directory in HDFS to store the EHR and United States regional datasets.

hadoop fs -mkdir /final-project

Copy datasetes from local filesystem to HDFS.

hadoop fs -copyFromLocal MU_REPORT_2016.csv /final-project
hadoop fs -copyFromLocal state_table.csv /final-project

Create two directories in the local filesystem for the Spark applications to process the EHR and United States regional datasets.

mkdir EhrData
mkdir StateTable

Create an sbt file in each directory.

Create an src subdirectory in each directory.

Create a main subdirectory in each src subdirectory.

Create a scala subdirectory in each main subdirectory.

Insert each scala file in each scala subdirectory.

Return to the directory containing the sbt file in each Spark application and run the following command to package the jar files.

sbt package

Inside the same directory run the following command to run the Spark applications in YARN client mode.

spark-submit --master yarn --deploy-mode client --num-executors 2 --driver-memory 500m --executor-memory 500m target/scala-2.10/ehrdata_2.10-1.0.jar
spark-submit --master yarn --deploy-mode client --num-executors 2 --driver-memory 500m --executor-memory 500m target/scala-2.10/statetable_2.10-1.0.jar

Enter the Hive shell.

hive

Create a database to hold the EHR and United States regional tables.

CREATE DATABASE final_project;

Create the EHR and United States regional tables.

CREATE TABLE ehrData (business_state_territory STRING, num_products INT, largest_provider STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE stateTable (business_state_territory STRING, region STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

Load in the data from the Spark applications

LOAD DATA INPATH '/final-project-output-ehrData/part-00000'
OVERWRITE INTO TABLE ehrData;

LOAD DATA INPATH '/final-project-output-stateTable/part-00000'
OVERWRITE INTO TABLE stateTable;

Join the two tables and query the results for regions within the United States that have the lowest number of EHR products.

SELECT e.*, s.region FROM ehrData e LEFT OUTER JOIN stateTable s
ON (e.business_state_territory = s.business_state_territory)
WHERE (s.region != "commonwealth" and s.region != "territory" and s.region != "minor")
ORDER BY num_products
LIMIT 5;
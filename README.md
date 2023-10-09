# Project for Creating an ETL Pipeline in Amazon AWS

## Project Background
### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Requirements
ETL Pipeline to extract data from S3, stage in Redshift, and transform the data into a set of dimensional table to allow the analytics team to continue finding insights into what songs the users are listerning to

![image](https://github.com/code-hy/udacity-cloud-data-warehouses/assets/82032854/e778199b-d0b1-451b-81e8-5cfe0f434395)

Schema for Song Play Analysis
Using the song and event datasets, a star schema is created for queries on song play analysis. This includes the following tables.

![image](https://github.com/code-hy/udacity-cloud-data-warehouses/assets/82032854/05dd82c6-acfb-4dd3-8a6e-b73806d247db)


Fact Table

*songplays* - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


Dimension Tables
*users* - users in the app
user_id, first_name, last_name, gender, level

*songs* - songs in music database
song_id, title, artist_id, year, duration

*artists* - artists in music database
artist_id, name, location, lattitude, longitude

*time* - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday


### Project Files


The project includes five files:

* create_table.py is where creation of staging, fact and dimension tables for the star schema in Redshift.
* etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
* sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
* data_profile.py is where you'll run select queries for count of records 
* README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

### Infrastructure as Code (setup_role_clients.ipynb and startup_cluster.ipynb)
1. import boto3 , json, pandas
2. update dwh.cfg to include the IAM user
3. Load DWH parameters from a file
4. Create clients for EC2, S3, IAM, and Redshift after importing boto3
5. ** Optional ** check out the sample data source for songs
6.     sampleDbBucketDend= s3.Bucket("udacity-dend")
       for obj in sampleDbBucketDend.objects.all():
            print(obj)
7. Create IAM role using iam.create_role
8. Attach Policy using iam.attach_role_policy (print out iam role)
9. Create a Redshift Cluster
10. Open incoming tcp port to access cluster endpoint
11. Connect to cluster using %load_ext sql and connection string

### ETL Redshift 
1. run %load_ext sql
2. Get parameters of the created redshift cluster (fill in endpoint and Iam role)
3. Connect to Redshift Cluster
4. Drop and Create Tables for partitioned data
5. Load Partitioned data into cluster using copy command
6. Drop and Create Tables for non-partitioned data
7. Load non-partitioned data into the cluster

### Creating Tables with Distribution Strategy and No-distribution strategy
1. run %load_ext sql
2. Get parameters of the created redshift cluster (fill in endpoint and Iam role)
3. Connect to Redshift Cluster
4. Create Tables for the Song and Event
5. Copy Tables from S3 supplying credentials and region to the tables using
        def load tables general function and with copy queries set-up
6.  Set up List of tables to be loaded
   
### Run Instructions
1.  In jupyter workspace, run all cells under setup_role_clients.ipynb in order to set up the roles 
2.  In jupyter workspace, run all cells under startup_cluster.ipynb to create redshift cluster
3.  Open up a terminal, and execute the create_tables.py (python create_tables.py) -> this will create all the staging,fact and dimension tables
4.  In terminal, execute the etl.py (python etl.py) -> this will populate the staging tables from s3, insert into the fact table from staging tables, and insert into dimension tables from staging tables
5.  In terminal, execute the data_profile.py (python data_profile.py)
6.  Go to Amazon console to pause the cluster, or delete the cluster if not needed anymore
   

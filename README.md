# udacity-cloud-data-warehouses
# project for creating an etl pipeline in amazon aws

## Project Instructions

Schema for Song Play Analysis
Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table
songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

Project Template
To get started with the project, go to the workspace on the next page, where you'll find the project template. You can work on your project and submit your work through this workspace.

Alternatively, you can download the template files in the Resources tab in the classroom and work on this project on your local computer.

The project template includes four files:

create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

Infrastructure as Code
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

ETL Redshift
1. run %load_ext sql
2. Get parameters of the created redshift cluster (fill in endpoint and Iam role)
3. Connect to Redshift Cluster
4. Drop and Create Tables for partitioned data
5. Load Partitioned data into cluster using copy command
6. Drop and Create Tables for non-partitioned data
7. Load non-partitioned data into the cluster

Creating Tables with Distribution Strategy and No-distribution strategy
1. run %load_ext sql
2. Get parameters of the created redshift cluster (fill in endpoint and Iam role)
3. Connect to Redshift Cluster
4. Create Tables for the Song and Event
5. Copy Tables from S3 supplying credentials and region to the tables using
        def load tables general function and with copy queries set-up
6.  Set up List of tables to be loaded
7.  

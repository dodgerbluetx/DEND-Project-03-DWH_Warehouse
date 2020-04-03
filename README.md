# Data Engineering Nanodegree
## Project 03 - Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song
database and want to move their processes and data onto the cloud. Their data
resides in S3, in a directory of JSON logs on user activity on the app, as well
as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that
extracts their data from S3, stages them in Redshift, and transforms data into
a set of dimensional tables for their analytics team to continue finding
insights in what songs their users are listening to. You'll be able to test
your database and ETL pipeline by running queries given to you by the analytics
team from Sparkify and compare your results with their expected results.

## Project Description

In this project, you'll apply what you've learned on data warehouses and AWS to
build an ETL pipeline for a database hosted on Redshift. To complete the
project, you will need to load data from S3 to staging tables on Redshift and
execute SQL statements that create the analytics tables from these staging
tables.

## Requirements

To run locally, an AWS account must be available and a user/key.  The AWS
details will be populated in dwh.cfg and are used to create a redshift cluster
and load the appropriate data.

Create a dwh.cfg file in the same directory as the scripts, and populate with
the following information:

~~~~
[CLUSTER]
HOST = <cluster host name, populate after creating cluster>
DB_NAME = dwh
DB_USER = <user name to use for db>
DB_PASSWORD = <pwd to use for db>
DB_PORT = 5439

[IAM_ROLE]
ARN = <iam role name, populate after creating cluster>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY = <key created in aws management console>
SECRET = <secret created in aws management console>

[DWH]
CLUSTER_TYPE = multi-node
NUM_NODES = 2
NODE_TYPE = dc2.large
CLUSTER_IDENTIFIER = dwhCluster
DB = dwh
DB_USER = <user name to use for db, same as above>
DB_PASSWORD = <pwd to use for db, same as above>
PORT = 5439
IAM_ROLE_NAME = dwhRole
~~~~


## Usage

Execute the following scripts in this order to fully retrieve and load data.

1. `manage_aws.py create` - This script will build an AWS Redshift cluster
   using the details provided in dwh.cfg.  There are some parameters returned
   to the terminal that will need to be populated into the dwh.cfg file once
   the cluster is fully built.

2. `create_tables.py` - This script will drop all tables is already existing,
   and then create the following tables:

   * `staging_events` - staging table for event data
   * `staging_songs` - staging table for song data
   * `songplays` - facts table for data on song play events
   * `users` - dimension table containing information about users using the
     music play app
   * `songs` - dimension table containing information about the songs in the db
   * `artists` - dimension table containing information about the artists gs in
     the db
   * `time` - dimension table containing information about the times songs were
     in the db

3. `etl.py` - This script will copy the data from the S3 bucket provided,
   stage the raw data in in staging tables created in step 2, and then insert
   specific data in the facts and dimensions tables also created in step 2.

4. `manage_aws.py delete` - This script will tear down the redshift cluster
   when complete, all data will be lost.  Be sure to run this when complete to
   ensure large charges are not accrued in AWS.

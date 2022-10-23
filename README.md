# Sparkify ETL Pipeline on AWS(Redshift)

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## Author

- [@Mohamed S. Elaraby](https://github.com/Aboalarbe)


## Role Description

we will build an ETL pipeline that extracts their data from AWS S3 buckets, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. we will be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## How to run the Python scripts

you should install psycopg2 package

```bash
pip install psycopg2
```

```bash
open CMD and run python create_tables.py
```

```bash
after that run python etl.py
```
    
## Description of files in the repository

### create_tables.py
contains scripts for creating the DB and Tables.

### etl.py
contains script for loading data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

### sql_queries.py
contains python scripts and SQL statement to create tables, insert data, select data and drop tables.

### Creating_AWS_Cluster.ipynb
- contains python scripts Create clients for IAM, EC2, S3 and Redshift.
- Clean up resources after finishing the process.

### dwh.cfg
contains configuration paramters for AWS, redshift cluster, DB and S3 buckets.

## Database Schema

![Schema](https://firebasestorage.googleapis.com/v0/b/plantsexpertsystem-f6812.appspot.com/o/Untitled%20Workspace.png?alt=media&token=52f7a554-c5db-4f01-94d7-a46e38645fde)
this schema created using "creatly.com"


## Feedback

If you have any feedback, please reach out to us at mhuss073@uottawa.ca


## 🔗 Links
[![portfolio](https://img.shields.io/badge/my_portfolio-000?style=for-the-badge&logo=ko-fi&logoColor=white)](https://www.credential.net/profile/mohamedaboalarbe/wallet)
[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohammed-elaraby/)

# pinterest-data-pipeline

This project aims to create a pinterest data pipeline using AWS cloud services. The data used in this scenario is an emulated version of real-life incoming data from the apps's API, and is stored in Amazon RDS. The project is built in such a way that an infinite loop is run which takes the data from the RDS storage and continuously sends it to a REST API which is connected to the various AWS services forming the pipeline. There are two parts in this project, incoming data being used for batch processing, and stream processing with near-real time analysis. 

## The overall architecture and steps
- An emulation script is run using python which fetches the data from AWS RDS and sends it to a REST API created in AWS API Gateway

- API is configured to do 2 things:
    1. Send data to AWS EC2 intance from which the data is then stored to an AWS S3 bucket
    2. Send the data to AWS Kinesis for stream processing  

- The stored data in S3 bucket is used for batch processing using databricks

- The Streaming data is accessed directly in databricks from the data streams and is processed and saved in delta tables


## Step - 1 : Data Exploration
Basic exploration of data, to identify the number of tables present, the structure in which it is stored and the different information/columns it contains. This is mainly done in order to get familiarised with the data, and more importantly the json payload to be sent must be structed in the form of column_name:value.


# Batch Processing Steps

## Step - 2 : Setting up EC2 instance
Before setting up an EC2 instance, we first need to create a MSK cluster as we will require the Bootstrap server string and the Apache Zookeeper connection string. Create an EC2 instance in AWS console and make sure to select the default security group witht he cluster VPC, and add "All Traffic" Type in the inbound rules.

Connect to the EC2 instance, and perform the following:
- Install java
- Install Kafka
- Inside Kafka installation folder download IAM MSK authentication package from GitHub.
- Create an environemnt variable for the above download so that Amazon MSK IAM libraries are accessible to Kafka client
- Configure the Kafka client properties to use AWS IAM authentication


## Step - 3 : Setup AWS MSK Connect
AWS MSK Connect allows users to stream data to and from their MSK-hosted Apache Kafka clusters. 
- Create an S3 bucket where the data for batch processing will be stored
- Create an IAM role with access to all functionalties for S3 bucket, and Kafka
- Create a VPC endpoint, which corresponds tot he MSK Cluster
- Create the connector in MSK console witht the IAM role previously created


## Step - 4 : Configuring API in AWS API Gateway
A REST API was configured and used for this project using the following steps:
- Create a REST type API from the API Gateway console
- Create a new child resource and configure it as proxy resource, and resource path as '{proxy+}'


## Step - 5 : Integrating API Gateway with Kafka on MSK
 - In the REST API configure the integration type
 - Enter the EC2 Instance's PublicDNS in the endpoint URL
 - Install Confluen in the EC2 instance
 - Modify the kafka-rest.properties file with the bootstrap server and zookeeper coonection string
 - Deploy the API and make a note of the invoke URL

This URL will be used for storing the incoming data to a S3 bucket, and later for batch processing. Start the REST Proxy in the EC2 instance, and once the server has started we can use python's requests library to test the API URL by sending data and obtaining a response.


## Processing in Databricks
Now that the data is stored in the S3 bucket, it can be accessed at regular intervals for batch processing. Firstly, create AWS Access Key and Secret Access Key in AWS IAM and upload the credentials file in Databricks for it to be able to access AWS services.

Mount the S3 bucket into Databricks and read data stored in the bucket and store them as dataframes. Thhe dataframes were cleaned using pyspark, and sql queries were performed to get the various analytics of the dataset.

To automate this process AWS MWAA (managed Worrkflows for Apache Airflow) can be used. Create an API token in databricks and intialize a connection between MWA and databricks. A DAG file can be created in python, which specifies the notebook path to be used from databricks, cluster id in AWS. It can be customised to run at regular intervals depending on the rate on incoming data. At the specified intervals, the notebook created will run automatically with the queties. An important part to consider here is that the stpes of mounting the S3 bucket should not be repeated which would causes an error, instead either the mounting process should be done separately or the bucket should be unmounted at the end of the notebook run.


# Stream Processing


## Step 6 : AWS Kinesis
AWS Kinesis is used to process streaming data and get near real-time analytics from the incoming data. Firstly, we create an IAM role for the API created earlier to access Kinesis, then following the steps:
- Create data streams using the Kinesis console, each set of incoming data will be sent to their respective streams

In API Gateway console, open the previously created REST API and perform the below steps:
- Create a new resource for the streams
- Setup and configure a GET, POST, and DELETE method
- Create two chile resources 'record' and 'records'
- Setup and configure a PUT method for both the above created child resources
- Deploy the API, and make a note of the new URL

Data can be sent to this new URL using the same procedure as previous step. The sent data can be visulaised in the Kinesis Data Streams in shards.


## Step - 7 : Integrating Kinesis with Databricks
The streaming data can be accessed in databricks from each of the data streams created. The will be read in serialized format and can be deserialized using the .selectEcpr() method. The schema of the dataframe is known fromt he inital data exploration and is created accordingly. The various transformation that were applied int he earlier steps can be applied here, which would give us the cleaned data.

The incoming data passes through all the above steps and keeps getting appended to the data frame. The analytics and queries to be performed and continuously repeated as the dataframe keeps getting updated. The final dataframe is stored in delta tables in Databricks for further use.

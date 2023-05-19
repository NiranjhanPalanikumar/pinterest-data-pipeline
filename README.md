# pinterest-data-pipeline

This project aims to create a pinterest data pipeline using AWS cloud services. The data used in this scenario is stored in Amazon RDS, and the project is built in such a way that an infinite loop is run which takes the data from the RDS storage and sends it to a REST API which is connected to the various AWS services.

## The overall architecture and steps
- An emulation script is run using python which fetches the data from AWS RDS and sends it to a REST API created in AWS API Gateway

- API is configured to do 2 things:
    1. Storing the data in a AWS S3 bucket
    2. Sending the data to AWS Kinesis 

- The stored data in S3 bucket is used for batch processing in databricks

- The Streaming data is accessed directly in databricks from the data streams and is processed and saved in delta tables


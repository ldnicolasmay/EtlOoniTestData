# ETL OONI Test Data

## Overview

This app is the second of three that fits into a pipeline for a larger project, an Open Observatory of Network Interference (OONI) Data Lake project.

## Purpose

The purpose of this app is to use the filtered S3 keys from the [FilterOoniS3Keys](https://github.com/ldnicolasmay/FilterOoniS3Keys) app to extract only relevant OONI test data from the S3 keys, transform the data into a star schema model, and load the data back into an S3 bucket that serves as the data lake.

This app takes care of the data in the middle third of the diagram that follow:

![Udacity Capstone Project Pipeline](img/UdacityCapstoneProject-Pipeline.svg "Udacity Capstone Project Pipeline")

## Running the App

### Setup

1. **Local Terminal**: Package Spark app

   ```shell script
   sbt package
   ```

   ```shell script
   EMR_URL="ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com" && \
     EMR_PEM="/home/hynso/aws_pems/spark-cluster-emr-us-east-1.pem"
   ```

   ```shell script
   rsync -au -e "ssh -i ${EMR_PEM}" \
     /home/hynso/Documents/Learning/DataEngineering/EtlOoniTestData/ \
     "hadoop@${EMR_URL}:/home/hadoop/EtlOoniTestData/"
   ```
2. **Remote Terminal**: Login to Amazon EMR cluster; Prepare home folder

   ```shell script
   EMR_URL="ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com" && \
     EMR_PEM="/home/hynso/aws_pems/spark-cluster-emr-us-east-1.pem"
   ```

   ```shell script
   ssh -i "${EMR_PEM}" "hadoop@${EMR_URL}"
   ```
   
   ```shell script
   cd EtlOoniTestData
   ```

### Run

1. **Remote Terminal**

   a. If you want to run the ETL Spark app, define the class for the OONI ETL main method:
   
   ```shell script
   APP_CLASS="EtlOoniTestData.OoniEtl"
   ```
   
   b. If you want to run the Query Spark app, define the class for the Query main method:
   
   ```shell script
   APP_CLASS="EtlOoniTestData.Query"
   ```
   
   Then run `spark-submit`:
   
   ```shell script
   spark-submit \
     --class "${APP_CLASS}" \
     --master yarn \
     --jars lib/aws-java-sdk-1.7.4.jar,lib/config-1.3.0.jar,lib/geny_2.11-0.1.6.jar,lib/os-lib_2.11-0.2.9.jar \
     target/scala-2.11/etloonitestdata_2.11-0.1.jar 
   ```
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=19:
                return False
            return True
        except:
            return False

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
   

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")

    clean_lines = lines.filter(good_line)
    
    #-9 to value as sha3_uncles is 11 char then minus FIRST two. And -1 from count because of the top row.
    dataSha = clean_lines.map(lambda x:  ("sha3_uncles", (len(x.split(",")[4])-2,1)))
    print(dataSha.take(10)) #check string output in logs
    dataSha = dataSha.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) #reduce tuple to total length and total instances in dataset i.e. rows
    
    #-8 to value as logs_bloom is 10 char then minus FIRST two. And -1 from count because of the top row 
    dataLogs = clean_lines.map(lambda x:  ("logs_bloom", (len(x.split(",")[5])-2,1)))
    print(dataLogs.take(10)) #check string output in logs
    dataLogs = dataLogs.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) #reduce tuple to total length and total instances in dataset i.e. rows
                            
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
   
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/sha3_uncles.txt')
    my_result_object.put(Body=json.dumps(dataSha.take(100)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/logs_bloom.txt')
    my_result_object.put(Body=json.dumps(dataLogs.take(100)))
                         
   
    spark.stop()
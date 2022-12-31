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
    
    def good_line_transactions(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[11])
            int(fields[8])
            return True
        except:
            return False
    
    def good_line_scams(line):
        try:
            fields = line.split(',')
            if len(fields)!=8:
                return False
            int(fields[0])
            return True
        except:
            return False
        
    def is_scamming(x):
        if x[1][1][1] == 'Scamming':
            return True
        
    def is_phishing(x):
        if x[1][1][1] == 'Phishing':
            return True
    
    def is_fico(x):
        if x[1][1][1] == 'Fake ICO':
            return True


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

    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    scams = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv")
    
    clean_scams = scams.filter(good_line_scams)

    clean_transactions = transactions.filter(good_line_transactions)
   
    transaction_features = clean_transactions.map(lambda b: (b.split(',')[6],(time.strftime("%m %y",time.gmtime(int(b.split(',')[11]))),int(b.split(',')[7])))) #(address, (date, value))
    
    scams_features = clean_scams.map(lambda b: (b.split(',')[6],(int(b.split(',')[0]),str(b.split(',')[4])))) #(address, (id, category))
   
    joined = transaction_features.join(scams_features) #(address, ((date, value), (id, category)))
   
    #most lucrative form of scam id
    scams_id = joined.map(lambda x: (x[1][1][0], x[1][0][1])) #(id, value)
    most_lucrative_scams = scams_id.reduceByKey(operator.add)
    top5scams = most_lucrative_scams.takeOrdered(5, key=lambda x: -x[1])
        
    # most lucrative forms of scam vs time
    #(date, total_value)
    scamming = joined.filter(is_scamming).map(lambda x: (x[1][0][0], x[1][0][1])).reduceByKey(operator.add).sortByKey(ascending=True)
    phishing = joined.filter(is_phishing).map(lambda x: (x[1][0][0], x[1][0][1])).reduceByKey(operator.add).sortByKey(ascending=True)
    fico = joined.filter(is_fico).map(lambda x: (x[1][0][0], x[1][0][1])).reduceByKey(operator.add).sortByKey(ascending=True)
    

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
   
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/most_lucrative_scams.txt')
    my_result_object.put(Body=json.dumps(top5scams))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/scamming.txt')
    my_result_object.put(Body=json.dumps(scamming.take(100))) 
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/phishing.txt')
    my_result_object.put(Body=json.dumps(phishing.take(100)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/fico.txt')
    my_result_object.put(Body=json.dumps(fico.take(100))) 
      
   
    spark.stop()
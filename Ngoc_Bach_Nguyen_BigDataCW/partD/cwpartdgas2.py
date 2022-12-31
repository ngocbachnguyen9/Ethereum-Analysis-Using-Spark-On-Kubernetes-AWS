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
       
    def good_line_contracts(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
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

    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    clean_transactions = transactions.filter(good_line_transactions)
    clean_contracts = contracts.filter(good_line_contracts)
   
    transaction_features = clean_transactions.map(lambda b: (b.split(',')[6],(time.strftime("%m %y",time.gmtime(int(b.split(',')[11]))),int(b.split(',')[8])))) #(address, (date, gas_used))
    contract_features = clean_contracts.map(lambda x: (x.split(",")[0], x.split(",")[1])) # (address, byte)
   
    joined = transaction_features.join(contract_features) # (address, ((date, gas_used), byte))
   
    monthly_average = joined.map(lambda x: (x[1][0][0], x[1][0][1]) ) # (date, gas_used)
    months = spark.sparkContext.broadcast(monthly_average.countByKey()) #no. of months
    monthly_average = monthly_average.reduceByKey(operator.add) 
    monthly_average = monthly_average.map(lambda x: (x[0], x[1]/months.value[x[0]]))  #(date, av. gas_used)
   
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
   
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/gas_monthly_average.txt')
    my_result_object.put(Body=json.dumps(monthly_average.take(100)))                            
   
    spark.stop()

import time
from time import sleep
import requests
import json
import pandas as pd
from json import dumps,loads
from kafka import KafkaProducer,KafkaConsumer
import fastparquet
from threading import Thread
from s3fs import S3FileSystem
import boto3
import configparser
import io

df_weather=pd.read_csv('weather.csv')
df_weather=df_weather[df_weather.province=='Ho Chi Minh City']  # fetch ho chi minh dataset only

# set up producer
def producer():
    producer=KafkaProducer(bootstrap_servers=['MAYTINH-UHGO2M2:9092']
                           ,value_serializer=lambda x:dumps(x).encode('utf-8'))

    while True: # simulate as live streaming
        dict_weather=df_weather.sample(1).to_dict(orient="records")[0]
        producer.send('demo1',dict_weather)

        sleep(3)

        producer.flush()

def consumer():
    consumer = KafkaConsumer('demo1', bootstrap_servers=['MAYTINH-UHGO2M2:9092'],
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
    df=[]
    row=0
    file_num=0

    # s3 Authentication
    #read configs
    config = configparser.ConfigParser()
    config.read('config.ini')

    mykey=config['aws']['mykey']
    mysecretkey=config['aws']['mysecretkey']
    region_name='ap-northeast-1'

    # function to upload parquet file on s3 bucket as dataframe
    def dataframe_to_parquet(s3_client,dataframe,bucketname,filename):
        csv_buffer = io.StringIO()
        dataframe.to_csv(csv_buffer)

        s3_client.put_object(ACL='private', Body=csv_buffer.getvalue(), Bucket=bucketname, Key=filename)

    s3=boto3.client('s3') # create client to s3

    for weather in consumer: # iterate through consumer
        df.append(weather)
        row+=1
        if row==5: # if reach 5 row in dataframe create new parquet file
            file_name='weather_file{}.parquet'.format(file_num)
            row=0
            dataframe=pd.DataFrame.from_dict(df) # create dataframe from dict
            dataframe.to_parquet(file_name,engine='fastparquet') # save local file


            dataframe_to_parquet(s3,dataframe,'weather-streaming-kafka',file_name) # upload into s3 bucket

            dataframe = df[0:0] # reset dataframe
            df.clear()
            file_num+=1


if __name__ == '__main__':
    # use thread to run 2 process concurrently
    Thread(target=producer).start()
    Thread(target=consumer).start()



# base path (D:\LEARN\Kafka\kafka_2.13-3.4.0\bin\windows)
# (run zookeeper_sever) zookeeper-server-start.bat ..\..\config\zookeeper.properties
# (run kafka_sever)     kafka-server-start.bat ..\..\config\server.properties
# (create topic only 1) kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic demo1
# ( start producer) kafka-console-producer.bat --topic demo1 --bootstrap-server MAYTINH-UHGO2M2:9092
# ( start consumer) kafka-console-consumer.bat --topic demo1 --bootstrap-server MAYTINH-UHGO2M2:9092
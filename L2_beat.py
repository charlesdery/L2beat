# -*- coding: utf-8 -*-
"""
Created on Tue Aug  3 17:06:02 2021

@author: charl
"""

import pandas as pd
import boto3
import configparser
import os
import sys

base_path = os.getcwd()
output_path = r"C:\Users\charl\Google Drive\2021\investing_programs\AWS Serverless Application\L2beat" + r"\output"
stamp = pd.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")
date = pd.to_datetime("today").strftime("%Y-%m-%d")
power_bi_file = r"L2beat_{}.csv".format(date)
s3_bucket = '1datadirectory'
s3_upload = r"L2beat/raw/"
source = r'L2BEAT'

# connect to S3

# credentials
config_file = r"C:\Users\charl\Google Drive\2021\investing_programs\config.txt"

parser = configparser.ConfigParser()
parser.read(config_file)

print(parser.get("AWS", "aws_access_key_id"))

s3 = boto3.resource(
    service_name='s3',
    region_name=parser.get("AWS", "region"),
    aws_access_key_id=parser.get("AWS", "aws_access_key_id"),
    aws_secret_access_key=parser.get("AWS", "aws_secret_access_key")
)

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

df = pd.read_html('https://l2beat.com/')

df = pd.DataFrame(df[0])
del df['No.']
df['Timestamp'] = stamp
df['Date'] = date
df['Source'] = source
#df['Value LockedTVL'] = df['Value LockedTVL'].astype(str)
df['Value LockedTVL'] = df['Value LockedTVL'].str.replace("$", "")
df['Value LockedTVL'] = df['Value LockedTVL'].str.replace(".", "")
df['Value LockedTVL'] = df['Value LockedTVL'].str.replace("K", "00")
df['Value LockedTVL'] = df['Value LockedTVL'].str.replace("M", "00000")
df['Value LockedTVL'] = df['Value LockedTVL'].str.replace("B", "00000000")
df['Value LockedTVL'] = df['Value LockedTVL'].astype(float)


df = df[['Date', 'Name', 'Value LockedTVL', '7 days change% / 7 days', 'Market share', 'Purpose', 'TechnologyTech',
         'Source', 'Timestamp']]

df.to_csv(output_path + '\\' + power_bi_file, index=False)

s3.Bucket(s3_bucket).upload_file(Filename=output_path + '\\' + power_bi_file, Key=s3_upload + power_bi_file,
                                 ExtraArgs={'ACL': 'public-read'})

print("L2 transaction data loaded successfully")
print(df.head())
sys.exit()

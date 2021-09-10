# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 10:17:43 2021

@author: Charles.Dery@outlook.com
"""

import pandas as pd
import glob
import boto3
import configparser
import sys

def concatenates_files(raw_data_path, processed_data_path,concatenated_file):
    """
    concatenates_files: will concatenate all files from a directory
    
    import pandas as pd
    import glob
    
    """
        
    #export to csv
    extension = 'csv'
    #dataset_location = os.chdir(r'D:\investing_programs\datasets\CoinMetrics')
    all_filenames = [i for i in glob.glob(raw_data_path +'\*.{}'.format(extension))]
    
    #combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
    #export to csv
    combined_csv.to_csv( processed_data_path +'\\'+ concatenated_file, index=False, encoding='utf-8-sig')
    
    
    
    #load csv file into a dataframe
    #combined_csv.to_csv( "coin_metrics_full_dataset.csv", index=False, encoding='utf-8-sig')
    
    print(concatenated_file + " Concatenated Dataset Created!")
    #create column with name of CSV file

#Archive to AWS
def upload_s3(output_path, output_file, s3_bucket, s3_repo):
    
    """
    upload_s3: upload file to S3
    
    import pandas as pd
    import boto3
    import configparser
    
    """
    config_file = r"C:\Users\charl\Google Drive\2021\investing_programs\config.txt"
    
    parser = configparser.ConfigParser()
    parser.read(config_file)
    
    print(parser.get("AWS", "aws_access_key_id"))
    
    s3 = boto3.resource(
        service_name='s3',
        region_name= parser.get("AWS", "region"),
        aws_access_key_id= parser.get("AWS", "aws_access_key_id"),
        aws_secret_access_key= parser.get("AWS", "aws_secret_access_key")
    )
    # Print out bucket names
    for bucket in s3.buckets.all():
        print(bucket.name)
    
    s3.Bucket(s3_bucket).upload_file(Filename= output_path + output_file, Key= s3_repo + output_file, ExtraArgs={'ACL':'public-read'})
   
    
    
    return (output_file + ' file saved')   



#Main
def main():

    #Filenames and path
    raw_data_path = r"D:\investing_programs\datasets\L2_beat\raw"
    processed_data_path = output_path = r"D:\investing_programs\datasets\L2_beat\processed"
    concatenated_file  =  output_file = r'concatenated_L2beat_dataset.csv'
    s3_bucket = '1datadirectory'
    s3_repo = 'L2beat/processed/'
    
    concatenates_files(raw_data_path, processed_data_path,concatenated_file)
    
    upload_s3(output_path +'\\' , output_file, s3_bucket, s3_repo)
    print(concatenated_file + " uploaded to S3!")
    sys.exit()
if __name__ == "__main__":
    main()

    


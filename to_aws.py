'''module that updloads data to the AWS S3 buckets'''

import os
import boto3
import logging
import json

#set up a basic error or Info log process
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename=f'{os.getcwd()}\\aws.log',
                    level=logging.DEBUG, format=LOG_FORMAT)

logger = logging.getLogger()
logger.info("Screen Scraper App Startup")


def aws_s3_upload(source, destination, filename, aws_file):
  ''' Function that uploads to an AWS S3 bucket
  source -> path to the file
  destination -> path to dropping off the file
  filename -> file that is being delivered
  awsFile -> json file containing the aws account credentials info
   '''
  try:
    with open(aws_file) as aws_creds:
      aws_config = json.load(aws_creds)
      aws_account = aws_config.get('lava_lamp_s3')[0].get('bucket_name')
      aws_access_key = aws_config.get('lava_lamp_s3')[0].get('aws_access_key')
      aws_secret_key = aws_config.get('lava_lamp_s3')[
          0].get('AWS_Secret_key')
      
      s3 = boto3.client(
          's3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
      s3.upload_file(source + filename,
                      aws_account, destination + filename)
      logging.info(f'Delivered to AWS, from: {source} to: {aws_account} {destination} {filename}')
  except Exception as err:
    logging.warning(f'Error with AWS S3 transmission: {err}')
    
  return

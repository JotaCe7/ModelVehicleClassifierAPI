import boto3
import os
import pandas as pd

import multiprocessing
from parallelbar import progress_imapu

# fetch credentials from env variables
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# s3 client
s3_client = boto3.client(
    's3', 
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    )

def list_s3_files_using_resource():
    """
    This functions list files from s3 bucket using s3 resource object.
    :return: None
    """

    s3 = boto3.resource(
        's3', 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # point the resource at the existing bucket
    s3_bucket = s3.Bucket("anyoneai-datasets")
    files = s3_bucket.objects.all()
    for file in files:
        print(file)

def list_s3_files_in_folder_using_client(prefix, client=s3_client):
    """
    This function will list down all files in a folder from S3 bucket
    Maximum of 1000 files listed
    :return: None
    """
    client = boto3.client(
        's3', 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    bucket_name = "anyoneai-datasets"
    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = response.get("Contents")
    for file in files:
      print(f"file_name: {file['Key']}, size: {file['Size']}")


def download_dir(prefix, local, bucket, client=s3_client):
    """
    Download all files from s3 bucket with folder structure.
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')            
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    print('Creating directories')
    for d in dirs:
        dest_pathname = os.path.join(local, *d.split('/')[1:])
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    print('Downloading files')
    for k in keys:
        dest_pathname = os.path.join(local, *d.split('/')[1:])
        client.download_file(bucket, k, dest_pathname)

###############################################################################################

def get_files_from_bucket(bucket, prefix, local, client=s3_client):
    """
    Get all the filenames from a s3 bucket and store then in a csv file
    params:
    - bucket: s3 bucket with target contents
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    df = pd.DataFrame()
    df['aws_path'] = keys
    df.drop(index=df.index[0], axis=0,inplace=True)
    df['path'] = df['aws_path'].str.split('/').str[-2:].str.join('/')
    df['directory'] =  df['aws_path'].str.split('/').str[-2]
    df['directory'] = df['directory'].str.replace('pro_ceed','proceed')
    df[['brand','model','year']] = df['directory'].str.split('_',expand=True)
    #df['directory'] = df['directory'].str.replace('proceed','pro_ceed').str.split('_',expand=True)
    df['filename'] =  df['aws_path'].str.split('/').str[-1]
    df['img_idx'] = df.groupby(df['filename'].str.split('_').str[0], sort=False).ngroup() + 1
    df['img_idx_2'] = df['filename'].str.split('_').str[0]
    print('df:', df)
    df.to_csv('./data.csv', index=False)
    return dirs, keys



# make a per process s3_client
def initialize():
  """
  Initializer function for multiprocessor task
  """
  global s3_client
  s3_client = boto3.client(
    's3', 
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    )

def download(job):
  """
  Work function of each process which will fetch something from s3
  """
  bucket, key, filename = job
  s3_client.download_file(bucket, key, filename)
  return

def download_files_parallel(bucket, data_path, out_path):
  """
  Download all files listed in a .csv file from s3 bucket to a main
  directory in parallel
  params:
    - bucket: s3 bucket with target contents
    - data_path: .scv file path
    - output_path: folder path where all files will be downloaded to
  """
  # Read .csv file as dataframe and get filenames to download
  df = pd.read_csv(data_path)
  keys = df['aws_path'].values
  filenames = out_path + df['filename'].astype(str)
  filenames = filenames.values
  # make the jobs
  jobs = [(bucket, key, filename) for key, filename in zip(keys, filenames) ]
  
  progress_imapu(download, jobs, initializer=initialize, n_cpu=multiprocessing.cpu_count(), core_progress=True)


#list_s3_files_in_folder_using_client("eu-car-dataset/")
#download_dir("eu-car-dataset/", "/home/app/src/data2", "anyoneai-datasets",s3_client)
get_files_from_bucket("anyoneai-datasets", "eu-car-dataset/", "/home/app/src/data", s3_client)
download_files_parallel("anyoneai-datasets", './data.csv', './images/')
import pandas as pd
import boto3
import json
import configparser


""" Clean up your resources """

### Step1 - Get Parameters

def get_configfile():
    print ("----------------1---------------")
    print ("1 Get AWS config")
   
    config = configparser.ConfigParser()
    config.read_file(open('dwh1.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    ## acess s3 data
    S3_UDACITY_REGION      = config.get("S3", "UDACITY_REGION")
    S3_UDACITY_BUCKET      = config.get("S3", "UDACITY_BUCKET")
    S3_LOG_DATA_FILTER     = config.get("S3", "LOG_DATA_FILTER")
    S3_SONG_DATA_FILTER    = config.get("S3", "SONG_DATA_FILTER")
    S3_LOG_JSONPATH_FILTER = config.get("S3", "LOG_JSONPATH_FILTER")
    S3_MYBUCKET            = config.get("S3", "MY_S3")

    
    ## DWH to dataframe

    dwh_list = ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", \
                "DWH_DB", "DWH_DB_USER",  \
                "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"]

    dwh_value = [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
                 DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
               
    dwh_db = pd.DataFrame({"Param": dwh_list, "Value": dwh_value })

    dwh_db.set_index('Param', inplace=True)

    
    ## S3 to dataframe

    s3_list = ["S3_UDACITY_REGION", "S3_UDACITY_BUCKET", "S3_LOG_DATA_FILTER", "S3_SONG_DATA_FILTER", \
               "S3_LOG_JSONPATH_FILTER", "S3_MYBUCKET"]

    s3_value = [S3_UDACITY_REGION, S3_UDACITY_BUCKET, S3_LOG_DATA_FILTER, S3_SONG_DATA_FILTER, S3_LOG_JSONPATH_FILTER, 
                S3_MYBUCKET]

               
    s3_db = pd.DataFrame({"Param": s3_list, "Value": s3_value})
    s3_db.set_index('Param')
    
    #print dwh param
    print ("-----------------------------------")
    print ("1.1 - DWH Param")
    print (dwh_db.to_markdown(), "\n")

    print ("1.2 - S3 Param")
    print (s3_db.to_markdown()) 
       
    ## get boto3 param
    iam = boto3.client('iam',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                    )
    s3 = boto3.resource('s3',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    redshift = boto3.client('redshift',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    ec2 = boto3.resource('ec2',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    s3cMyb = boto3.client('s3',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    s3Myb = boto3.resource('s3',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    boto3_param = [iam, s3, redshift, ec2, s3cMyb, s3Myb]
    
    ## return config param
    return config, boto3_param


""" -----------------------------------------------------------------------
STEP 2: Delete Redshift Cluster 
"""

def delete_cluster(config, boto3_param):
    print ("----------------2---------------")
    print ("2 Delete Redshift Cluster")
    redshift = boto3_param[2]
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    
    try:
        result = redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print ('2.1 Delete Redshift Cluster Error', e)

    ## waiting delete cluster
    print ("2.1 Waiting delete cluster")

    myClusterProps = {}
    available = True
    while available:
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        except Exception as e:
            print (e)
            available = False
        else:
            if myClusterProps['ClusterStatus'] != 'deleting':   
                available = False
    
    print ("2.3 - Cluster deleted")
 

##-- delete manifest file and bucket
def delete_bucket(config, boto3_param):
    print ("----------------3---------------")
    print ("3 Delete MyBucket")
    
    s3cMyb = boto3_param[4]
    S3_MYBUCKET  = config.get("S3", "MY_S3")
    
    try: 
        result = s3cMyb.delete_object(Bucket=S3_MYBUCKET, Key='song.manifest')
    except Exception as e:
        print ("3.1 Delete Manifest file error:", e)
    else:
        print ("3.1 Manifest file deleted")

    ##-- delete bucket
    try:
        s3cMyb.delete_bucket(Bucket=S3_MYBUCKET)              
    except Exception as e:
        print ("3.2 Delete Bucket Error:", e)
    else:
        print ("3.2 Bucket deleted")
               
               
#-- Detach Policies from IAM
def detach_policies(config, boto3_param):
    print ("----------------4---------------")    
    print ("4 Detach Policies from IAM")
               
    iam = boto3_param[0]
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
              
    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, 
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    except Exception as e:
        print ("4.2 Detach policy AmazonS3ReadOnlyAccess Error:", e)
    else:
        print ("4.2 Detached policy AmazonS3ReadOnlyAccess")
    

    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, 
                           PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess")
    except Exception as e:
        print ("4.3 Detach policy AmazonRedshiftAllCommandsFullAccess Error:", e)
    else:
        print ("4.3 Detached policy AmazonRedshiftAllCommandsFullAccess")
           
    try:
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except Exception as e:
        print ("4.4 Delete", DWH_IAM_ROLE_NAME, "Error:", e)
    else:
        print ("4.4 Deleted", DWH_IAM_ROLE_NAME)
   
        
""" === Main === """
def main():
    ## get configuration and Key
    config, boto3_param = get_configfile()
    
    ## delete Redshift Cluster
    delete_cluster(config, boto3_param)
                  
    ##-- delete manifest file and bucket
    delete_bucket(config, boto3_param)
    
    #-- Detach Policies from IAM
    detach_policies(config, boto3_param)           

    print ("-------- AWS Resouces desconnected ----------")   
    
    
if __name__ == "__main__":
    main()
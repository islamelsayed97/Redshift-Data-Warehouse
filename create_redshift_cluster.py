import boto3
import json
import time
import configparser
import pandas as pd


# AWS config parameters
KEY = None
SECRET = None

DWH_CLUSTER_TYPE = None
DWH_NUM_NODES = None
DWH_NODE_TYPE = None

DWH_CLUSTER_IDENTIFIER = None
DWH_DB = None
DWH_DB_USER = None
DWH_DB_PASSWORD = None
DWH_PORT = None

DWH_IAM_ROLE_NAME = None


def config_parser():
    """
    Set the AWS Config parameters with values from dwh.cfg config file
    """

    global KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES
    global DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB
    global DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME

    print("Parsing the configuration file...\n")

    config = configparser.ConfigParser()
    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')

        DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
        DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

        DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

        DWH_DB = config.get("CLUSTER", "DB_NAME")
        DWH_DB_USER = config.get("CLUSTER", "DB_USER")
        DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
        DWH_PORT = config.get("CLUSTER", "DB_PORT")

def aws_client(service, region):
    """
    Creates an AWS client (specified by the argument) in region (specified by argument)
    
    Params:
    service -- The service to be created
    region -- The region where service has to be created
   
    Returns
    client -- The client for AWS service
    """

    client = boto3.client(service,
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET,
                          region_name=region)

    return client


def aws_resource(service, region):
    """
    Creates an AWS resource (specified by the argument) in region (specified by argument)
    
    Params:
    service -- The resource to be created
    param region -- The region where resource has to be created
    
    Returns
    resource -- The resource for AWS service
    """

    resource = boto3.resource(service,
                              region_name=region,
                              aws_access_key_id=KEY,
                              aws_secret_access_key=SECRET)

    return resource

# create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

def create_iam_role(iam):
    """
    Create the AWS IAM role and attach AmazonS3ReadOnlyAccess role policy to this IAM role
    
    Params:
    iam -- Boto3 client for IAM
    
    Returns: 
    roleArn -- IAM role ARN string
    """
    
    try:
        print('1.1 Creating a new IAM Role...')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        dwhRole = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
    
    # attaching Policy 
    print('1.2 Attaching Read Only Access Policy...')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']
                          
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    return roleArn

# create redshift cluster 
def create_redshift_cluster(redshift, roleArn):
    """
    Initiate the AWS Redshift cluster creation process
    
    Params:
    redshift -- Boto3 client for the Redshift
    roleArn -- The ARN string for IAM role
    
    Returns 
    boolean -- True if the cluster was created successfully, False otherwise.
    """

    print('2. Creating Redshift cluster...')
    try:
        response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,

            #Identifiers and credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
        
        cluster_status = response['ResponseMetadata']['HTTPStatusCode']
        print(f'Redshift cluster creation http response status code: {cluster_status}')
        return cluster_status == 200
        
    except Exception as e:
        print(e)
        return False
        
        
        
def config_update_cluster(redshift):
    """
    Write the cluster endpoint and IAM ARN string to the dwh.cfg configuration file
    
    Params:
    redshift -- Boto3 client for Redshift
    """

    print("Writing the cluster endpoint address and IAM Role ARN to the config file...\n")
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    config = configparser.ConfigParser()
    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

    config.set("CLUSTER", "HOST", cluster_props['Endpoint']['Address'])
    config.set("IAM_ROLE", "ARN", cluster_props['IamRoles'][0]['IamRoleArn'])

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)


def redshift_cluster_status(redshift):
    """
    Retrieves the Redshift cluster status
    
    Params:
    redshift -- Boto3 client for Redshift
    
    Returns
    cluster_status -- The cluster status
    """

    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus'].lower()

    return cluster_status
    
    
# open an incoming TCP port to access the cluster ednpoint
def aws_open_redshift_port(ec2, redshift):
    """
    Opens an incoming TCP port to access Redshift cluster endpoint on VPC security group
    
    Params:
    ec2 -- Boto3 client for EC2 instance
    Redshift -- Boto3 client for Redshift
    """
    print('3. Openning an incoming TCP port to access the cluster ednpoint...')
    clusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    vpc = ec2.Vpc(id=clusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]


    try:
        vpc = ec2.Vpc(id=clusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        
        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
        
        
if __name__ == '__main__':
    
    config_parser()
    
    region = 'us-west-2'

    ec2 = aws_resource('ec2', region)
    s3 = aws_resource('s3', region)
    iam = aws_client('iam', region)
    redshift = aws_client('redshift', region)

    roleArn = create_iam_role(iam)

    clusterCreationStarted = create_redshift_cluster(redshift, roleArn)
    
    if clusterCreationStarted:
        print('The cluster is being created...')
        print('Checking if the cluster is created')
        time.sleep(5)
        cluster_status = redshift_cluster_status(redshift)
        print(f'Cluster status: {cluster_status}')
        print('Cluster is still being created. Please wait this process may take a few minutes.')

        
        while True:
            cluster_status = redshift_cluster_status(redshift)
            if cluster_status == 'available':
                print(f'Cluster status: {cluster_status}')
                time.sleep(2)
                config_update_cluster(redshift)
                aws_open_redshift_port(ec2, redshift)
                break
          
            time.sleep(10)
        print('Cluster was created successfully.\n')
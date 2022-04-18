import boto3
import configparser
import time

# AWS config parameters
KEY = None
SECRET = None

DWH_CLUSTER_IDENTIFIER = None


def config_parser():
    """
    Set the AWS Config parameters with value from dwh.cfg config file
    """

    global KEY, SECRET, DWH_CLUSTER_IDENTIFIER

    print("Parsing the configuration file...\n")

    config = configparser.ConfigParser()
    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')

        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")


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


def redshift_cluster_status(redshift):
    """
    Creates an AWS resource (specified by the argument) in region (specified by argument)
    
    Params:
    service -- The resource to be created
    param region -- The region where resource has to be created
    
    Returns
    resource -- The resource for AWS service
    """

    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus'].lower()

    return cluster_status        
        

def destroy_redshift_cluster(redshift):
    """
    Destroy the Redshift cluster
    
    Params
    redshift -- Boto3 client for Redshift
    """
    
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)


if __name__ == '__main__':
    config_parser()

    region = 'us-west-2'
    redshift = aws_client('redshift', region)
    
    cluster_status = redshift_cluster_status(redshift)

    if cluster_status == 'available':
        print(f'Cluster status: {cluster_status}')
        destroy_redshift_cluster(redshift)
        
        while cluster_status == 'available':
            cluster_status = redshift_cluster_status(redshift)
        
        print('The cluster is being deleted...')
        time.sleep(5)
        print('Cluster status: ', cluster_status)
        time.sleep(2)
        print("Cluster is still being deleted. Please wait until that's done.")
        
    while True:
        try:
            cluster_status = redshift_cluster_status(redshift)
            time.sleep(10)
        except Exception:
            print("The cluster is not available now.")
            break
        
    
  
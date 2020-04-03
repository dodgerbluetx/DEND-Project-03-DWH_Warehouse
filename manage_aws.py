import pandas as pd
import boto3
import json
import time
import configparser


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = [
        "ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername",
        "DBName", "Endpoint", "NumberOfNodes", 'VpcId'
    ]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def create_cluster(
    KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER,
    DB, DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME, ec2, iam, redshift
):
    # create the role
    try:
        print("Creating a new IAM Role...")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'redshift.amazonaws.com'
                    }
                }],
                'Version': '2012-10-17'
            })
        )
    except Exception as e:
        print(e)

    # attach the policy
    print("Attaching Policy...")
    iam.attach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    # show the IAM role ARN
    print("Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    print()

    try:
        response = redshift.create_cluster(
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),
            DBName=DB,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)

    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=CLUSTER_IDENTIFIER
    )['Clusters'][0]

    while myClusterProps['ClusterStatus'] != 'available':
        print("Waiting for cluster to become available...")
        print()
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=CLUSTER_IDENTIFIER
        )['Clusters'][0]
        print(prettyRedshiftProps(myClusterProps))
        print()
        time.sleep(5)
    else:
        print("Cluster is up and running!")
        print("Address: {}".format(myClusterProps['Endpoint']['Address']))
        print()

    ENDPOINT = myClusterProps['Endpoint']['Address']
    ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("ENDPOINT :: ", ENDPOINT)
    print("_ROLE_ARN :: ", ROLE_ARN)

    # test tcp connection into cluster
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(PORT),
            ToPort=int(PORT)
        )
    except Exception as e:
        print(e)


def delete_cluster(
    KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER,
    DB, DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME, ec2, iam, redshift
):

    redshift.delete_cluster(
        ClusterIdentifier=CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
    )

    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=CLUSTER_IDENTIFIER
    )['Clusters'][0]

    while myClusterProps['ClusterStatus'] == 'deleting':
        print("Waiting for cluster to go away...")
        print()
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=CLUSTER_IDENTIFIER
        )['Clusters'][0]
        print(prettyRedshiftProps(myClusterProps))
        print()
        time.sleep(5)
    else:
        print("Cluster is gone!")
        print()

    iam.detach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam.delete_role(RoleName=IAM_ROLE_NAME)


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    CLUSTER_TYPE = config.get("DWH", "CLUSTER_TYPE")
    NUM_NODES = config.get("DWH", "NUM_NODES")
    NODE_TYPE = config.get("DWH", "NODE_TYPE")
    CLUSTER_IDENTIFIER = config.get("DWH", "CLUSTER_IDENTIFIER")
    DB = config.get("DWH", "DB")
    DB_USER = config.get("DWH", "DB_USER")
    DB_PASSWORD = config.get("DWH", "DB_PASSWORD")
    PORT = config.get("DWH", "PORT")
    IAM_ROLE_NAME = config.get("DWH", "IAM_ROLE_NAME")

    pd.DataFrame({
        "Param":[
            "CLUSTER_TYPE", "NUM_NODES", "NODE_TYPE", "CLUSTER_IDENTIFIER",
            "DB", "DB_USER", "DB_PASSWORD", "PORT", "IAM_ROLE_NAME"
        ],
        "Value":[
            CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER, DB,
            DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME
        ]
    })

    ec2 = boto3.resource(
        'ec2',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    iam = boto3.client(
        'iam',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name='us-west-2'
    )

    redshift = boto3.client(
        'redshift',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    create_cluster(
        KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER,
        DB, DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME, ec2, iam, redshift
    )

    delete_cluster(
        KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER,
        DB, DB_USER, DB_PASSWORD, PORT, IAM_ROLE_NAME, ec2, iam, redshift
    )


if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print()
    print("Execution Time: {}".format(end - start))
    print()

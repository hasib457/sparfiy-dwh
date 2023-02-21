# pylint: disable=all
"""
Creates an Amazon Redshift cluster on AWS.

The module uses a configuration file and other functions to create an Amazon Redshift cluster on AWS.
The configuration file specifies parameters for the cluster, such as
    - the type and number of nodes
    - database name
    - cluster identifier
    - database username and password.
The other functions include
    - `config()`, which reads the configuration file;
    - `resources()`, which creates AWS resources using the AWS SDK for Python (`boto3`);
    - `create_iam_role()`, which creates an IAM role for the cluster;
    - `create_redshift_cluster()`, which creates the Redshift cluster itself;
    - `open_ports()`, which opens ports for the cluster;
    - `create_resources()`, which can be used to create the cluster and its associated resources.
    - `delete_resources()`, which can be used to delete the cluster and its associated resources.

Note that the `delete_resources()` or `create_resources()` functions shoud by specified in run command
    - `python3 create_dwh_infrastructure.py --create` for create resources
    - `python3 create_dwh_infrastructure.py --delete` for delete resources

"""
import configparser
import json
import time
import argparse

import boto3
from botocore.exceptions import ClientError


def config():
    """
    Reads a configuration file and returns a dictionary of the required arguments to create an Amazon Redshift cluster.
    """
    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")

    DWH_CLUSTER_TYPE = config.get("CLUSTER", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("CLUSTER", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("CLUSTER", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("CLUSTER", "DB_NAME")
    DWH_DB_USER = config.get("CLUSTER", "DB_USER")
    DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_PORT = config.get("CLUSTER", "DB_PORT")
    VPCID = config.get("CLUSTER", "VPCID")

    DWH_IAM_ROLE_NAME = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")

    return {
        "DWH_CLUSTER_TYPE": DWH_CLUSTER_TYPE,
        "DWH_NUM_NODES": DWH_NUM_NODES,
        "DWH_NODE_TYPE": DWH_NODE_TYPE,
        "DWH_CLUSTER_IDENTIFIER": DWH_CLUSTER_IDENTIFIER,
        "DWH_DB": DWH_DB,
        "DWH_DB_USER": DWH_DB_USER,
        "DWH_DB_PASSWORD": DWH_DB_PASSWORD,
        "DB_PORT": DB_PORT,
        "DWH_IAM_ROLE_NAME": DWH_IAM_ROLE_NAME,
        "KEY": KEY,
        "SECRET": SECRET,
        "VPCID": VPCID
    }


def resources(KEY, SECRET):
    """
    Creates the required AWS resources - the IAM, Redshift, and EC2 clients - and returns them in a dictionary.
    """
    iam = boto3.client(
        "iam",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )
    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    ec2 = boto3.resource(
        "ec2",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    return {"iam": iam, "redshift": redshift, "ec2": ec2}


def create_iam_role(iam, DWH_IAM_ROLE_NAME, KEY, SECRET):
    """
    Creates an IAM role for the Redshift cluster and attaches a policy. It then returns the ARN of the IAM role.
    """
    try:
        print("[INFO] Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)

    # Attaching Policy
    print("[INFO] Attaching Policy")

    _ = iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("[INFO] Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]
    print(roleArn)

    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))
    config.set("IAM_ROLE", "ARN", roleArn)

    # write the changes to the configuration file
    with open("dwh.cfg", "w") as configfile:
        config.write(configfile)
    return roleArn


def create_redshift_cluster(
    redshift,
    roleArn,
    DWH_CLUSTER_TYPE,
    DWH_NODE_TYPE,
    DWH_NUM_NODES,
    DWH_DB,
    DWH_CLUSTER_IDENTIFIER,
    DWH_DB_USER,
    DWH_DB_PASSWORD,
):
    """
    Creates a Redshift cluster using the given arguments and returns the cluster endpoint and IAM role ARN.
    """
    try:
        print("[INFO] Create redshift cluster")
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[roleArn],
        )
    except Exception as e:
        print(e)

    cluster_status = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]["ClusterStatus"]

    print("[INFO] Check cluster status")

    while cluster_status != "available":
        time.sleep(60)
        cluster_status = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]["ClusterStatus"]

    cluster_props = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]

    DWH_ENDPOINT = cluster_props["Endpoint"]["Address"]
    DWH_ROLE_ARN = cluster_props["IamRoles"][0]["IamRoleArn"]
    VPCID = cluster_props["VpcId"]
    print("[INFO] Cluster is available now")
    print("[INFO] DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("[INFO] DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))
    
    config.set("CLUSTER", "HOST", DWH_ENDPOINT)
    config.set("CLUSTER", "VPCID", VPCID)
    config.set("CLUSTER", "DWH_ROLE_ARN", DWH_ROLE_ARN)

    # write the changes to the configuration file
    with open("dwh.cfg", "w") as configfile:
        config.write(configfile)

    return cluster_props


def open_ports(ec2, cluster_props, DB_PORT, KEY, SECRET):
    """
    Opens an incoming TCP port to access the cluster and returns the security group.
    """
    try:
        print("[INFO] OPEN TCP PORTS")
        vpc = ec2.Vpc(id=cluster_props["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        # print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT),
        )
    except Exception as e:
        print(e)
def revoke_ports(ec2, VPCID, DB_PORT, KEY, SECRET):
    """
    Revoke an incoming TCP port to access the cluster and returns the security group.
    """
    try:
        print("[INFO] REVOKE TCP PORTS")
        vpc = ec2.Vpc(id=VPCID)
        defaultSg = list(vpc.security_groups.all())[0]
        # print(defaultSg)
        defaultSg.revoke_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT),
        )
    except Exception as e:
        print(e)


def delete_resources():
    """
    Deletes the Redshift cluster and its associated resources. 
    Note that this function should be used with caution as it deletes all the resources created by create_resources(). 
    It should be called with the --delete option.
    """
    config_prams = config()
    aws_resources = resources(config_prams["KEY"], config_prams["SECRET"])
    # delete cluster
    print("[INFO] Delete redshift cluster")
    try:
        aws_resources["redshift"].delete_cluster(
            ClusterIdentifier=config_prams["DWH_CLUSTER_IDENTIFIER"], SkipFinalClusterSnapshot=True
        )
    except Exception as e:
        print(e)

    # delete role
    print("[INFO] Delete redshift role")
    try:
        aws_resources["iam"].detach_role_policy(
            RoleName=config_prams["DWH_IAM_ROLE_NAME"],
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )
    except Exception as e:
        print(e)

    try:
        aws_resources["iam"].delete_role(RoleName= config_prams["DWH_IAM_ROLE_NAME"])
    except Exception as e:
        print(e)

    # revoke ports
    revoke_ports(
        aws_resources["ec2"],
        config_prams['VPCID'],
        config_prams["DB_PORT"],
        config_prams["KEY"],
        config_prams["SECRET"],
    )
def create_resources():
    """
    Creates the Redshift cluster and its associated resources.
    """

    config_prams = config()
    aws_resources = resources(config_prams["KEY"], config_prams["SECRET"])
    roleArn = create_iam_role(
        aws_resources["iam"],
        config_prams["DWH_IAM_ROLE_NAME"],
        config_prams["KEY"],
        config_prams["SECRET"],
    )

    cluster_props = create_redshift_cluster(
        aws_resources["redshift"],
        roleArn,
        config_prams["DWH_CLUSTER_TYPE"],
        config_prams["DWH_NODE_TYPE"],
        config_prams["DWH_NUM_NODES"],
        config_prams["DWH_DB"],
        config_prams["DWH_CLUSTER_IDENTIFIER"],
        config_prams["DWH_DB_USER"],
        config_prams["DWH_DB_PASSWORD"],
    )
    open_ports(
        aws_resources["ec2"],
        cluster_props,
        config_prams["DB_PORT"],
        config_prams["KEY"],
        config_prams["SECRET"],
    )


def main():

    parser = argparse.ArgumentParser(description='Script for creating and deleting Redshift culster')
    parser.add_argument('--create', action='store_true')
    parser.add_argument('--delete', action='store_true')
    args = parser.parse_args()

    if args.create:
        print("[INFO] CREATING RESOURCES")
        create_resources()
    elif args.delete:
        print("[INFO] DELETING RESOURCES")
        delete_resources()
    else:
        print("No action specified. Please specify either --create or --delete.")



if __name__ == "__main__":
    main()

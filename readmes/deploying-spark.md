# Deploying Spark

The goal of this lab is to understand how to deploy Spark applications, locally or on a remote AWS cluster.
For this purpose, **we will switch from Python to Scala**; you will notice that APIs are more or less the same.

To deploy a Spark application, we will use **Intellij IDEA Ultimate** (see license instructions in the main REAMDE) and you will need to:
- Compile the code into a JAR file.
- Setup a Run/Debug Configuration in IDEA to run the JAR using a (local or remote) *spark-submit* application.
- Run the application.

Once executed, the history is stored as JSON file in a preconfigured folder.
To visualize it, you need to run and access the History Server application.

### The jobs

Inside the folder "src/main/scala/utils" you will find:
- A *Commons* file with some utility functions.
- A *Config* file where you need to insert:
  - The path to this repo in the `projectDir` property.
  - The name of the S3 bucket you will create in the `s3bucketName` property.

Inside the folder "src/main/scala/lab104" you will find:
- A *WordCount* file, containing a simple Word Count example on the divinacommedia.txt dataset.
- The *MovieLens* file that (together with the *MovieLensParser*) contains the Scala version of the Spark jobs we did in Lab103.

The jobs are designed to easily switch from local to remote deployments.

### Index

- [Local deployment](#local-deployment)
  - [Requirements](#requirements)
  - [History Server](#history-server)
  - [Deployment](#deployment)
- [Remote deployment](#remote-deployment)
  - [Requirements](#requirements-1)
  - [Accessing the Virtual Lab on AWS Academy](#accessing-the-virtual-lab-on-aws-academy)
  - [AWS CLI configuration](#aws-cli-configuration)
  - [Upload datasets on S3](#upload-datasets-on-s3)
  - [Create a cluster with EMR](#create-a-cluster-with-emr)
  - [Deployment](#deployment-1)

## Local deployment

This section covers the local deployment.

### Requirements

All computers in the laboratories are pre-configured with everything you need. 
If you want to work on you local computer, follow the instructions in the [Spark setup](/readmes/spark-setup.md) 
README to install and configure everything you need.

> NOTE: you do *not* need the Docker image used in previous labs

### History Server

Open a terminal on Spark's installation folder ("C:/spark" in lab machines) and launch this command.

```
bin/spark-class.cmd org.apache.spark.deploy.history.HistoryServer
```

Spark's UI will be available at [http://localhost:18080](http://localhost:18080).

### Deployment

- From a terminal positioned on the main folder of the repo, run `./gradlew` to generate the JAR. 
You should see it in the "build/libs" folder; if the process fails, delete the folder and try again. 
- Define a new Run/Debug Configuration with the following settings:
  - Configuration type: Spark Submit - Local     (deprecated)
  - Name: Spark Local
  - Spark home: should be found automatically
  - Application: point to the .jar file inside the "build/libs" folder
  - Class: lab104.WordCount (or whichever application you want to run)
  - Run arguments: `local`
  - Cluster manager: Local
  - Master: `local[n]` where `n` is the number of cores you assign to the driver
- Hit the Run button and wait for the application to finish (you should see "SparkContext is stopping with exitCode 0." in the log).
- Check out the results in the "output" folder and the job execution plans in the History Server. 


## Remote deployment

This section covers the remote deployment on AWS using the AWS Academy.

### Requirements

For this part, you need an account on AWS Academy. 
This service offers *classes* where students are allowed free usage of some AWS resources up to 50$; 
registration is necessary, but no credit card is needed.

Classes have a **duration of 6 months**; after that, all resources will be lost. To give you access for the full academic year, there will be two classes:
- Class 1: valid from September 15, 2025, to March 15, 2026.
- Class 2: valid from March 16, 2026, to September 14, 2026.

Invites are sent to all students registered to the course on [Virtuale](https://virtuale.unibo.it) according to this modality:
- Invites to Class 1 are sent on October 1, 2025, based on who has registered on Virtuale by September 30, 2025
- Invites to Class 2 are sent on March 1, 2026, based on who has registered on Virtuale by February 28, 2026.

**Invites are not automatic!** If you register on Virtuale later than the indicated dates, you must send the teacher an email to request access.  

When you receive the invitation link, proceed with the registration. Contact the teacher in case of issues.

### Accessing the Virtual Lab on AWS Academy

The Virtual Lab must be turned on every time you want to work with the AWS resources. **It turns off automatically after 4 hours**, unless you ask for an extension of 4 more hours.

- [**Access AWS Academy here**](https://awsacademy.instructure.com/)
- Go to "Modules" > "Launch AWS Academy Learner Lab"
  - Agree to the terms and conditions (only the first time)
- Click the "Start Lab" button (top-right corner)
  - The first time, it will take about 5 minutes to startup; next times, it will take only a few seconds
- A *countdown* will appear, starting from 4 hours
  - **To extend the 4-hour window**, click the "Start Lab" button again
- A *credit counter* will appear, indicating the amount of credit left
- Click on "AWS" (top-left corner) when you have a green light
  - You will now be in your own virtual lab
  - This is the ***Console home*** of the Virtual Lab
- **Click the "End Lab"** button (top-right corner) at the end of class
  - If you don't, it should automatically shutdown in 4 hours, but 1) it will consume credit (only EC2 instances are automatically shut down), and 2) better safe than sorry.

#### Troubleshooting (Q&A)

>*Q: What happens when I end the Virtual lab?*  
>A: It depends on the service: data in S3 will persist (and consume very little credit even when the Virtual Lab is down); EMR clusters (and all data within them, e.g., anything you save onto HDFS) will be completely destroyed.
>
>*Q: What happens when I finish my credits?*  
>A: You will not be able to use the Virtual Lab anymore, not even the data in S3. However, you can request an additional Virtual Lab environment to be instantiated on a different account (with a different email), just ask the teacher.

After launching the Virtual Lab, you can interact with AWS resources in two alternative ways.
- GUI: it's nicer and doesn't require you to learn commands and syntax; 
however, it's slow and the layout can change unpredictably.
- AWS CLI (preferred): it requires you to learn commands and syntax, 
but it is less subject to evolution issues; also, it is useful to understand how
the interaction with AWS could be automated (infrastructure-as-code)

We will use the AWS CLI. 

### AWS CLI configuration

To use the AWS CLI, you need to create a profile and a key-pair (.ppk). 
Use a Git Bash opened on the root directory of this repo for the bash commands below.


#### Profile creation and update

>**IMPORTANT** - 
> Profile information (access key, secret access key, and token) must be updated every time you restart the Virtual Lab.

In the "aws" folder of this repo, rename the "credentials-example" file to "credentials" and insert the following:
- AWS Access Key ID
- AWS Secret Access Key
- AWS Session Token

All these can be found in the AWS Acamedy website (where you click the "Start Lab" button); 
here, look for AWS details > AWS CLI > aws_access_key_id (and the other fields are just below).

Next, set the environment variable pointing to the profile.

```bash
export AWS_SHARED_CREDENTIALS_FILE=aws/credentials
export AWS_CONFIG_FILE=aws/config
```

#### Key-pair creation

Key pairs are needed to launch Spark applications. 
Once created, you can reuse them as much as you want, but you cannot download them again.
If you lose the file, you need to create a new key pair.

Issue the following commands to:
- Create a key pair (replace <my_key_pair> with a name of your choice)
- Show the list of created keys

```bash
aws ec2 create-key-pair --key-format ppk --key-name <my_key_pair> --output text > aws/keys/<my_key_pair>.ppk
aws ec2 describe-key-pairs
```

#### Troubleshooting (Q&A)

>*Q: I get a 400 error when creating the key-pair.*  
>A: The profile and/or the token may have been copy-pasted incorrectly.
> Try to recreate them, even if you are sure that you did it correctly.
>
>*Q: I get an error message stating that the .ppk is not valid.*  
>A: The .ppk may be incorrect; check that:
>- it begins with "PuTTY-User-Key-File-2: ssh-rsa"
>- it ends with the Private-MAC
>
>*Q: I get an error message stating that the access permissions on the .ppk are not valid.*  
>A: Change permissions to make the file accessible only by the owner; for this, use the command ```chmod 400 file.ppk```.


### Upload datasets on S3

Amazon S3 (Simple Storage Service) is the basic storage service offered by Amazon.

> **IMPORTANT**: all datasets used in previous labs have already been uploaded 
> to an open-access bucket called `unibo-bd2526-egallinucci-shared`, 
> so you do not need to upload them too. Still, you must create your own bucket to:
> - Save jobs' output
> - Upload the datasets you will use for the project

Documentation for CLI commands is available [here (lower-level commands)](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html#cli-aws-s3) and [here (higher-level commands)](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/index.html#cli-aws-s3api).
Here is a list of the most common ones.

- See available buckets 
  ```bash
  aws s3api list-buckets
  ```

- **Create your bucket**; replace <my_bucket_name> with something like *unibo-bd2526-egallinucci*
  ```bash
  aws s3api create-bucket --bucket <my_bucket_name>
  ```

- **Create datasets folders**
  ```bash
  aws s3api put-object --bucket <my_bucket_name> --key datasets/
  aws s3api put-object --bucket <my_bucket_name> --key datasets/big/
  ```

- Copy, rename, and delete folders
  ```  bash
  aws s3 cp s3://<my_bucket_name>/datasets/ s3://<my_bucket_name>/somefoldername/
  
  aws s3 mv s3://<my_bucket_name>/somefoldername/ s3://<my_bucket_name>/foldertodelete/

  aws s3 rm s3://<my_bucket_name>/foldertodelete/
  ```

- **Upload datasets** (all the files that you find in the "datasets" folder)
  ```bash
  aws s3api put-object --bucket <my_bucket_name> --key datasets/capra.txt --body datasets/capra.txt
  # Repeat for other files as well
  aws s3api put-object --bucket <my_bucket_name> --key datasets/big/ml-movies.csv --body datasets/big/ml-movies.csv
  # Repeat for other big files as well
  ```

- Check contents of bucket and folders
  ```bash
  aws s3api list-objects-v2 --bucket <my_bucket_name>
  aws s3api list-objects-v2 --bucket <my_bucket_name> --prefix datasets/
  ```

All these files will continue to be available even after the Virtual Lab is ended.


### Create a cluster with EMR

Amazon EMR (Elastic Map Reduce) is the service used to deploy clusters preconfigured with Hadoop-stack software (e.g., HDFS, Spark, Kafka, etc.).
It relys on Amazon EC2 (Elastic Compute Cloud), which is the (more general) service used to request machines.
Documentation is available [here](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/emr/index.html#cli-aws-emr). To decide which EMR versions to load, see [here](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html).


Enable inbound SSH connections to the Security Group of the Master node 
(**must be done only once, but one cluster must have been created first** - see next command).
This is needed to submit the job from Intellij IDEA (or if you ever need to access machines via SSH).

```bash
aws ec2 authorize-security-group-ingress --group-name ElasticMapReduce-master --ip-permissions IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges="[{CidrIp=0.0.0.0/0}]"
```

Create a cluster with the following command.
- This is the default configuration for laboratories, but you can change it for the project (e.g., to increase the computational power if needed)
- See other examples [here](https://github.com/aws/aws-cli/blob/develop/awscli/examples/emr/create-cluster-examples.rst)
- **Make sure that you replace the key-pair name in the command**
- Note that the command returns the cluster_id (which could be needed for other commands) 

```
aws emr create-cluster \
    --name "Big Data Cluster" \
    --release-label "emr-7.11.0" \
    --applications Name=Hadoop Name=Spark \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large InstanceGroupType=CORE,InstanceCount=2,InstanceType=m4.large \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,KeyName=<my_key_pair_name> \
    --region "us-east-1"
```

Show the last deployed clusters; to run jobs, you must wait for the status of the cluster 
to go from **STARTING** to **WAITING** (it takes some minutes).

```bash
aws emr list-clusters --max-items 1
```

Terminate a cluster.

```bash
aws emr terminate-clusters --cluster-ids <cluster_id>
```

### Deployment

- Under src/main/resources, create a file called "aws_credentials.txt"; 
put here the value of your *aws_access_key* in the first line 
and the value of your *aws_secret_access_key* in the second line (see AWS CLI configuration).
  - The content of this file will need to be updated every time you restart the Virtual Lab 
- From a terminal positioned on the main folder of the repo, run `./gradlew` to generate the JAR.
  You should see it in the "build/libs" folder; if the process fails, delete the folder and try again.
- Define a new Run/Debug Configuration with the following settings:
  - Configuration type: Spark Submit - Cluster
  - Name: Spark Cluster
  - Region: us-east-1
  - Remote Target: Add EMR connection
    - Authentication type: Profile from credentials file
    - Select "Set custom config" and give the paths to the "config" and "credential" files 
    - Click on "Test connection" to verify
  - Enter a new SSH Configuration
    - Host: the address of the primary node of the cluster, i.e., the MasterPublicDnsName (see [AWS CLI cheatsheet]())
    - Username: hadoop
    - Authentication type: Key pair
    - Private key file: point to your .ppk
    - Test the connection
  - Application: point to the .jar file   inside   the build/libs folder of this   repository; if   you don't find it, build the   project
  - Class: lab104.WordCount
  - Run arguments: ```remote``` (if you want to read input datasets from your bucket)
    or ```sharedRemote``` (if you want to read input datasets from the open-access bucket)
  - Before launch: Upload Files Through SFTP
- Hit the Run button and wait for the application to finish (you should see "final status: SUCCEEDED" in the last Application report).
- Check out the results in the "output" folder in your S3 bucket (via CLI or GUI).
  - From the *Console home* of the Virtual Lab, select the S3 service, click on the bucket 
    and navigate to the output folder
  - Data cannot be viewed from the GUI, you must download the file(s)
- Check out the job execution plans in the History Server (via GUI). 
  - From the *Console home* of the Virtual Lab, select the EMR service, click on the 
    last-created cluster and click on the link to the "Spark History Server"

> Note: according to your preferences/habits, you may choose to:
> - Put input and output directories as parameters of the Debug/Run Configuration instead of hard-coding them (though this means changing the configuration every time, or creating many of them)
> - Add a "build" step in the "Before launch" option of the Debug/Run Configuration to avoid the error of running old jars (though you will incur in additional time if jar you want to run didn't need to be rebuilt)
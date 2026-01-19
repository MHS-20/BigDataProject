## Not Optimized
### First Shuffle
Aggregare i dati basandosi sull'ip sorgente, calcolo la somma dei bytes e della durata. 

### Second Shuffle
join tra:
- ```trafficWithIP : RDD[(String, TrafficRecord)]```
- ```ipProfileRDD : RDD[(String, IPProfile)]```

- In pratica ottengo:
```
EnrichedRecord(
  record: TrafficRecord,
  profile: IPProfile
  )
```
Ogni connessione viene arricchita con il profilo dell'ip sorgente e con le informazioni aggregate 
(aggiungere il profilo ip ha senso, ma aggiungere anche tutte le informazioni aggregate non tanto)

### Third Shuffle
Non posso riusare i dati aggregati dal primo shuffle, perché non ho contato le label malicious/benign.
Quindi devo ricalcolare i dati aggregati ma questa volta aggregando per profilo ip e label.

## Optimized
Se nel primo shuffle conto anche il numero di label benign/malicious, 
poi basta mettere il profilo come chiave insieme alla label,
ed aggregare su quelle (questa allora sarà la versione ottimizzata)

## Remote Deployment
History Server (gitbash):
```
cd C:\spark-3.5.3-bin-hadoop3
bin/spark-class.cmd org.apache.spark.deploy.history.HistoryServer
```
Spark's UI will be available at http://localhost:18080.

Copy the dataset to S3:
```
aws s3 cp datasets/dataset18-2.csv s3://mhs-lab1/datasets/datasetIoT.csv --region us-east-1 --no-verify-ssl
```

Get Master Public DNS:
```
aws emr list-clusters --max-items 1
aws emr describe-cluster --cluster-id j-20LZK4UL5O77V --query "Cluster.MasterPublicDnsName" --output text
```

Monitor Job: 
```
aws emr list-steps --cluster-id j-XXXXXXXXXXXXX --step-states PENDING RUNNING
aws emr list-steps --cluster-id j-XXXXXXXXXXXXX --step-states COMPLETED FAILED CANCELLED
```

Custom cluster resources:
```
aws emr create-cluster \
    --name "Big Data Cluster" \
    --release-label "emr-7.11.0" \
    --applications Name=Hadoop Name=Spark \
    --instance-groups \
    InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
    InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,KeyName=my_key_pair \
    --region "us-east-1"
```

Check for results: 
```
aws s3api list-objects-v2 --bucket mhs-lab1
aws s3api list-objects-v2 --bucket mhs-lab1 --prefix datasets/
```

Spark submit to EMR:
```
--num-executors 2
--executor-cores 2
--executor-memory 6G
--driver-memory 4G

--num-executors 2
--executor-cores 4
--executor-memory 12G
--driver-memory 4G
--conf spark.sql.shuffle.partitions=8
```

j-3FVG8GH9MP1FX
ec2-54-80-25-6.compute-1.amazonaws.com

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
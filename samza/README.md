## Steps to build and deploy the samza jobs

### Pre-requisites:
**Grid(Zookeeper, Kafka, Hadoop Cluster) should be up and running**

### Steps
1. Build the sunbird-lms-jobs project
```
mvn clean install
```

2. Create a folder to extract samza job tar.gz file
```
cd ~/samza/jobs/ 
mkdir indexer
```

3. Extract the samza job tar.gz file in the new folder
```
cd indexer
tar -xvf {project_path}/sunbird-lms-jobs/samza/indexer/target/samza.indexer-0.0.1-distribution.tar.gz -C .
```

4. Deploy the job
```
./bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=./config/local.indexer.properties
```


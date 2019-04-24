# Setup instructions for different Jobs

## Setup-indexer
1. Goto samza directory.

2. Run ./bin/grid bootstrap

3. Build project  using "mvn clean install"
4. Use command "mkdir depoy/samza" .
5. Use command "tar -xvf indexer/target/samza.indexer-0.0.1-distribution.tar.gz - C deploy/samza" 
6. Create kafka topic "local.lms.audit.events".
7. All the job configuration details can be found in "sunbird-lms-jobs/samza/indexer/src/main/config" directory.
8. Use command "deploy/samza/bin/run-job.sh  --config-path=file://$PWD/deploy/samza/config/local.indexer.properties" for running the job.
9. Any message pushed to kafka topic "local.lms.audit.events" will be consumed by indexer job.
10. We can access yarn cluster on "localhost:8088" and see the status, logs and other details of our job.
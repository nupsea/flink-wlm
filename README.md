# flink-wlm

    
### Job Run 

- Download the latest Flink stable release from https://flink.apache.org/downloads.html
- Extract it into your DEV space.

- Start the cluster

`./bin/start-cluster.sh`

- Check the Dashboard of the Flink Cluster running on:  _localhost:8081_

- Build the jar

- Copy/Deploy the jar on your local standalone cluster DEV space.

- Run the interested Job: 

`./bin/flink run -c org.nupsea.flink.batch.SampleJob flink-wlm-<ver>.jar --data <full-path-of-data-dir>`

`./bin/flink run -c org.nupsea.flink.batch.student.StudentAnalyser flink-wlm-<ver>.jar --data <full-path-of-data-dir>`

`./bin/flink run -c org.nupsea.flink.batch.product.ProductsAnalyser flink-wlm-<ver>.jar --data <full-path-of-data-dir>`

- View the job runs in the Flink Dashboard.

###
###

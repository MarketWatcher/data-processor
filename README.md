# MarketWatcher Data Processor

## Running
```bash
KAFKA_ZOO_KEEPER=<zoo_keeper> GROUP=<consumer-group> TOPICS=<topics> NUM_THREADS=<num-threads> CASSANDRA_NODES=<cassandra-nodes> sbt "run-main TwitterProcessor"

```


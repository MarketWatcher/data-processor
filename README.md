# MarketWatcher Data Processor

### Running
```bash
KAFKA_BROKER=<broker> GROUP=<consumer-group> TOPICS=<topics> NUM_THREADS=<num-threads> CASSANDRA_NODES=<cassandra-nodes> sbt "run-main TwitterProcessor"

```


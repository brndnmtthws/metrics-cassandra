A Codahale Metrics reporter for Cassandra 2.0
=================
[![Build Status](https://travis-ci.org/brndnmtthws/metrics-cassandra.svg?branch=master)](https://travis-ci.org/brndnmtthws/metrics-cassandra)

This is a Cassandra reporter for the Codahale Metrics library (https://github.com/codahale/metrics).  It uses the DataStax Java driver (https://github.com/datastax/java-driver) to write metrics to Cassandra.

## Grab it from [clojars.org](https://clojars.org/)

```xml
  <repositories>
    <repository>
      <id>clojars.org</id>
      <url>http://clojars.org/repo</url>
    </repository>
  </repositories>
  ...
  <dependencies>
    <dependency>
      <groupId>org.clojars.brenden</groupId>
      <artifactId>metrics-cassandra</artifactId>
      <version>3.0.2</version>
    </dependency>
  </dependencies>
```

## Try it out

```java
final Cassandra cassandra = new Cassandra(
		Arrays.asList("cassandra.example.com"),
		"metrics", /*   keyspace          */
		"data",    /*   table             */
		31536000,  /*   TTL in seconds    */
		9042,      /*   native port       */
		"ONE");    /*   consistency level */

final CassandraReporter reporter = CassandraReporter.forRegistry(registry)
	.prefixedWith("metrics")
	.convertRatesTo(TimeUnit.SECONDS)
	.convertDurationsTo(TimeUnit.MILLISECONDS)
	.filter(MetricFilter.ALL)
	.build(cassandra);

reporter.start(1, TimeUnit.MINUTES);
```

The code above will create 2 tables in the `metrics` keyspace: one called `data`, and one called `data_names`.  The schema for these tables looks like this:

```sql
CREATE TABLE IF NOT EXISTS data (
  name VARCHAR,
  timestamp TIMESTAMP,
  value DOUBLE,
  PRIMARY KEY (name, timestamp)
);
CREATE TABLE IF NOT EXISTS data_names (
  name VARCHAR,
  last_updated TIMESTAMP,
  PRIMARY KEY (name)
);
```

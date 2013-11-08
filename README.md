A Codahale Metrics reporter for Cassandra 2.0
=================
This is a Cassandra reporter for the Codahale Metrics library (https://github.com/codahale/metrics).  It uses the DataStax Java driver (https://github.com/datastax/java-driver) to write metrics to Cassandra.

Try it out:

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

Grab it from clojars.org:

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
      <version>3.0.1</version>
    </dependency>
  </dependencies>
```

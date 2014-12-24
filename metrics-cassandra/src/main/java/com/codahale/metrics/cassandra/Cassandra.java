package com.codahale.metrics.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.List;

/**
 * A client to a Carbon server.
 */
public class Cassandra implements Closeable {
  private static final Pattern UNSAFE = Pattern.compile("[\\.\\s]+");

  private final String keyspace;
  private final String table;
  private final int ttl;

  private Cluster cluster;
  private int failures;
  private boolean initialized;
  private Session session;
  private ConsistencyLevel consistency;
  private Map<String, PreparedStatement> preparedStatements;

  private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra.class);

  /**
   * Creates a new client which connects to the given address and socket factory.
   *
   * @param addresses Contact point of the Cassandra cluster
   * @param keyspace Keyspace for metrics
   * @param table name of metric table
   * @param ttl TTL for entries
   * @param port Port for Cassandra cluster native transport contact point
   * @param consistency Consistency level to attain
   */
  public Cassandra(List<String> addresses, String keyspace, String table,
      int ttl, int port, String consistency) {

    this.keyspace = keyspace;
    this.table = table;
    this.ttl = ttl;
    this.consistency = ConsistencyLevel.valueOf(consistency);

    this.cluster = build(addresses, port);

    this.initialized = false;
    this.failures = 0;
    this.preparedStatements = new HashMap<String, PreparedStatement>();
  }
  
  /**
   * Creates a new client using an existing cluster
   * @param cluster cluster to use
   * @param keyspace Keyspace for metrics
   * @param table name of metric table
   * @param ttl TTL for entries
   * @param consistency Consistency level to attain
   */
  public Cassandra(Cluster cluster, String keyspace, String table, int ttl, String consistency)
  {
    this.keyspace = keyspace;
    this.table = table;
    this.ttl = ttl;
    this.consistency = ConsistencyLevel.valueOf(consistency);

    this.cluster = cluster;

    this.initialized = false;
    this.failures = 0;
    this.preparedStatements = new HashMap<String, PreparedStatement>();
  }

  private static Cluster build(List<String> addresses, int port) {
    Cluster.Builder builder = Cluster.builder();
    for (String address : addresses) {
      builder.addContactPoint(address);
    }
    builder
      .withPort(port)
      .withCompression(Compression.LZ4)
      .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
      .withLoadBalancingPolicy(LatencyAwarePolicy.builder(new RoundRobinPolicy()).build());
    
    Cluster cluster = builder.build();
    
    try {
      // Attempt to init the cluster to make sure it's usable. I'd prefer to remove this and leave it on the
      // client to retry when the connect method throws an exception.
      cluster.init();
      return cluster;
    } catch(NoHostAvailableException e) {
      LOGGER.warn("Unable to connect to Cassandra, will retry contact points next time",
          cluster, e);
      cluster = builder.build();
      cluster.init();
    }
    
    return cluster;
    
  }

  /**
   * Connects to the server.
   *
   */
  public void connect() {
    preparedStatements.clear();
    session = cluster.connect(keyspace);
  }

  /**
   * Sends the given measurement to the server.
   *
   * @param name      the name of the metric
   * @param value     the value of the metric
   * @param timestamp the timestamp of the metric
   * @throws DriverException if there was an error sending the metric
   */
  public void send(String name, Double value, Date timestamp) throws DriverException {
    try {
      if (session == null) connect();
      String tableName = sanitize(table);
      if (!initialized) {
        session.execute(new SimpleStatement(
              "CREATE TABLE IF NOT EXISTS " + tableName + " (      " +
              "  name VARCHAR,                                     " +
              "  timestamp TIMESTAMP,                              " +
              "  value DOUBLE,                                     " +
              "  PRIMARY KEY (name, timestamp))                    " +
              "  WITH bloom_filter_fp_chance=0.100000 AND          " +
              "  compaction = {'class':'LeveledCompactionStrategy'}")
        );
        session.execute(new SimpleStatement(
              "CREATE TABLE IF NOT EXISTS " + tableName + "_names (   " +
              "  name VARCHAR,                                        " +
              "  last_updated TIMESTAMP,                              " +
              "  PRIMARY KEY (name))                                  " +
              "  WITH bloom_filter_fp_chance=0.100000 AND             " +
              "  compaction = {'class':'LeveledCompactionStrategy'}   ")
        );
        initialized = true;
      }

      if (!preparedStatements.containsKey("values-" + tableName)) {
        preparedStatements.put("values-" + tableName,
            session.prepare(
                "INSERT INTO " + tableName + " (name, timestamp, value) VALUES (?, ?, ?) USING TTL ?")
                .setConsistencyLevel(consistency));
      }
      if (!preparedStatements.containsKey("names-" + tableName)) {
        preparedStatements.put("names-" + tableName,
            session.prepare(
                "UPDATE " + tableName + "_names SET last_updated = ? WHERE name = ?")
                .setConsistencyLevel(consistency));
      }

      session.execute(
          preparedStatements.get("values-" + tableName).bind(
            name, timestamp, value, ttl)
      );

      session.execute(
          preparedStatements.get("names-" + tableName).bind(
            timestamp, name).setConsistencyLevel(consistency)
      );

      this.failures = 0;
    } catch (DriverException e) {
      close();
      failures++;
      throw e;
    }
  }

  /**
   * Returns the number of failed writes to the server.
   *
   * @return the number of failed writes to the server
   */
  public int getFailures() {
    return failures;
  }

  @Override
  public void close() throws DriverException {
    if (session != null) {
      session.close();
      session = null;
    }
  }

  protected String sanitize(String s) {
    return UNSAFE.matcher(s).replaceAll("_");
  }
}

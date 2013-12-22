package com.codahale.metrics.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.BatchStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.List;

/**
 * A client to a Carbon server.
 */
public class Cassandra implements Closeable {
  private static final Pattern UNSAFE = Pattern.compile("[\\.\\s]+");

  private final List<String> addresses;
  private final int port;
  private final String keyspace;
  private final String table;
  private final int ttl;
  private final ConsistencyLevel consistency;

  private Cluster cluster;
  private int failures;
  private boolean initialized;
  private Session session;
  private BatchStatement batch;

  private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra.class);

  /**
   * Creates a new client which connects to the given address and socket factory.
   *
   * @param address Contact point of the Cassandra cluster
   * @param keyspace Keyspace for metrics
   * @param table name of metric table
   * @param ttl TTL for entries
   * @param port Port for Cassandra cluster native transport contact point
   * @param consistency Consistency level to attain
   */
  public Cassandra(List<String> addresses, String keyspace, String table,
      int ttl, int port, String consistency) {
    this.addresses = addresses;
    this.port = port;

    this.keyspace = keyspace;
    this.table = table;
    this.ttl = ttl;
    this.consistency = ConsistencyLevel.valueOf(consistency);

    this.cluster = build();

    this.initialized = false;
    this.failures = 0;
  }

  private Cluster build() {
    Cluster.Builder builder = Cluster.builder();
    for (String address : addresses) {
      builder.addContactPoint(address);
    }
    return builder
      .withPort(port)
      .withCompression(Compression.LZ4)
      .build();
  }

  /**
   * Connects to the server.
   *
   */
  public void connect() {
    try {
      session = cluster.connect(keyspace);
    } catch (NoHostAvailableException e) {
      LOGGER.warn("Unable to connect to Cassandra, will retry contact points next time",
          cluster, e);
      cluster = build();
    }
    batch = new BatchStatement();
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
      String tableName = sanitize(table);
      if (!initialized) {
        session.execute(new SimpleStatement(
              "CREATE TABLE IF NOT EXISTS " + tableName + " (   " +
              "  name VARCHAR,                                  " +
              "  timestamp TIMESTAMP,                           " +
              "  value DOUBLE,                                  " +
              "  PRIMARY KEY (name, timestamp))")
        );
        session.execute(new SimpleStatement(
              "CREATE TABLE IF NOT EXISTS " + tableName + "_names (   " +
              "  name VARCHAR,                                        " +
              "  last_updated TIMESTAMP,                              " +
              "  PRIMARY KEY (name))")
        );
        initialized = true;
      }

      batch.add(new SimpleStatement(
            "INSERT INTO " + tableName + " (name, timestamp, value) VALUES (?, ?, ?) USING TTL ?",
            name, timestamp, value, ttl)
      );

      batch.add(new SimpleStatement(
            "UPDATE " + tableName + "_names SET last_updated = ? WHERE name = ?",
            timestamp, name)
      );

      this.failures = 0;
    } catch (DriverException e) {
      failures++;
      throw e;
    }
  }

  public void execute() {
    try {
      batch.setConsistencyLevel(consistency);
      session.executeAsync(batch);
    } catch (DriverException e) {
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
    session.shutdown();
  }

  protected String sanitize(String s) {
    return UNSAFE.matcher(s).replaceAll("_");
  }
}

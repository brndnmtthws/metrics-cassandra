package com.codahale.metrics.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;

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

	private final Cluster cluster;
	private final String keyspace;
	private final String table;
	private final int ttl;
	private final ConsistencyLevel consistency;

    private int failures;
	private boolean initialized;
	private Session session;

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
		Cluster.Builder builder = Cluster.builder();
		for (String address : addresses) {
			builder.addContactPoint(address);
		}
		this.cluster = builder
			.withPort(port)
			.withCompression(Compression.LZ4)
			.build();

		this.keyspace = keyspace;
                this.table = table;
		this.ttl = ttl;
		this.consistency = ConsistencyLevel.valueOf(consistency);

		this.initialized = false;
		this.failures = 0;
	}

    /**
     * Connects to the server.
     *
     */
    public void connect() {
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
			String tableName = sanitize(table);
			if (!initialized) {
				session.execute(new SimpleStatement(
							"CREATE TABLE IF NOT EXISTS " + tableName + " (   " +
							"  name VARCHAR,                                  " +
							"  timestamp TIMESTAMP,                           " +
							"  value DOUBLE,                                  " +
							"  PRIMARY KEY (name, timestamp))")
						.setConsistencyLevel(consistency)
						);
				session.execute(new SimpleStatement(
							"CREATE TABLE IF NOT EXISTS " + tableName + "_names (   " +
							"  name VARCHAR,                                        " +
							"  last_updated TIMESTAMP,                              " +
							"  PRIMARY KEY (name))")
						.setConsistencyLevel(consistency)
						);
				initialized = true;
			}

			session.execute(new SimpleStatement(
						"INSERT INTO " + tableName + " (name, timestamp, value) VALUES (?, ?, ?) USING TTL ?",
						name, timestamp, value, ttl)
					.setConsistencyLevel(consistency)
					);

			session.execute(new SimpleStatement(
						"UPDATE " + tableName + "_names SET last_updated = ? WHERE name = ?",
						timestamp, name)
					.setConsistencyLevel(consistency)
					);

            this.failures = 0;
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

package com.codahale.metrics.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;

import java.io.*;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * A client to a Carbon server.
 */
public class Cassandra implements Closeable {
    private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");

	private final Cluster cluster;
	private final String keyspace;
	private final Long ttl;
	private final ConsistencyLevel consistency;

    private int failures;

	private Session session;
	private HashSet<String> tableSet;

    /**
     * Creates a new client which connects to the given address and socket factory.
     *
     * @param address Contact point of the Cassandra cluster
	 * @param keyspace Keyspace for metrics
	 * @param ttl TTL for entries
	 * @param port Port for Cassandra cluster native transport contact point
	 * @param consistency Consistency level to obtain
     */
	public Cassandra(InetAddress address, String keyspace, Long ttl, int port, String consistency) {
		this.cluster = Cluster.builder()
			.addContactPoints(address)
			.withPort(port)
			.withCompression(Compression.SNAPPY)
			.build();

		this.keyspace = keyspace;
		this.ttl = ttl;
		this.consistency = ConsistencyLevel.valueOf(consistency);

		this.tableSet = new HashSet<String>();
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
     * @throws IOException if there was an error sending the metric
     */
    public void send(String table, String name, Double value, Date timestamp) throws IOException {
        try {
			String cleanTable = sanitize(table);
			if (!tableSet.contains(cleanTable)) {
				session.execute(new SimpleStatement(
							"CREATE TABLE IF NOT EXISTS ? (   " +
							"  name VARCHAR,                  " +
							"  timestamp TIMESTAMP,           " +
							"  value DOUBLE,                  " +
							"  PRIMARY KEY (name, time))      ",
							cleanTable)
						.setConsistencyLevel(consistency)
						);
			}

			session.execute(new SimpleStatement(
						"INSERT INTO ? (name, timestamp, value) VALUES (?, ?, ?) USING TTL ?",
						cleanTable, name, timestamp, value, ttl)
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
        return WHITESPACE.matcher(s).replaceAll("-").replaceAll("\\.", "_");
    }
}

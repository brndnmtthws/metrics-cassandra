package com.codahale.metrics.cassandra;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.exceptions.DriverException;

import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.Date;

/**
 * A reporter which publishes metric values to a Cassandra server.
 *
 */
public class CassandraReporter extends ScheduledReporter {
  /**
   * Returns a new {@link Builder} for {@link CassandraReporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link CassandraReporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder for {@link CassandraReporter} instances. Defaults to not using a prefix, using the
   * default clock, converting rates to events/second, converting durations to milliseconds, and
   * not filtering metrics.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private Clock clock;
    private String prefix;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.clock = Clock.defaultClock();
      this.prefix = null;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    /**
     * Use the given {@link Clock} instance for the time.
     *
     * @param clock a {@link Clock} instance
     * @return {@code this}
     */
    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Prefix all metric names with the given string.
     *
     * @param prefix the prefix for all metric names
     * @return {@code this}
     */
    public Builder prefixedWith(String prefix) {
      this.prefix = prefix;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Builds a {@link CassandraReporter} with the given properties, sending metrics using the
     * given {@link Cassandra} client.
     *
     * @param cassandra a {@link Cassandra} client
     * @return a {@link CassandraReporter}
     */
    public CassandraReporter build(Cassandra cassandra) {
      return new CassandraReporter(registry,
          cassandra,
          clock,
          prefix,
          rateUnit,
          durationUnit,
          filter);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraReporter.class);

  private final Cassandra cassandra;
  private final Clock clock;
  private final String prefix;

  private CassandraReporter(MetricRegistry registry,
      Cassandra cassandra,
      Clock clock,
      String prefix,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      MetricFilter filter) {
    super(registry, "cassandra-reporter", filter, rateUnit, durationUnit);
    this.cassandra = cassandra;
    this.clock = clock;
    this.prefix = prefix;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    final Date timestamp = java.util.Calendar.getInstance().getTime();

    try {
      cassandra.connect();

      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        reportGauge(entry.getKey(), entry.getValue(), timestamp);
      }

      for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        reportCounter(entry.getKey(), entry.getValue(), timestamp);
      }

      for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
        reportHistogram(entry.getKey(), entry.getValue(), timestamp);
      }

      for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        reportMetered(entry.getKey(), entry.getValue(), timestamp);
      }

      for (Map.Entry<String, Timer> entry : timers.entrySet()) {
        reportTimer(entry.getKey(), entry.getValue(), timestamp);
      }

      cassandra.execute();
    } catch (DriverException e) {
      LOGGER.warn("Unable to report to Cassandra", cassandra, e);
    } finally {
      try {
        cassandra.close();
      } catch (DriverException e) {
        LOGGER.warn("Error disconnecting from Cassandra", cassandra, e);
      }
    }
  }

  private void reportTimer(String name, Timer timer, Date timestamp) throws DriverException {
    final Snapshot snapshot = timer.getSnapshot();

    cassandra.send(prefix(name, "max"), format(convertDuration(snapshot.getMax())), timestamp);
    cassandra.send(prefix(name, "mean"), format(convertDuration(snapshot.getMean())), timestamp);
    cassandra.send(prefix(name, "min"), format(convertDuration(snapshot.getMin())), timestamp);
    cassandra.send(prefix(name, "stddev"),
        format(convertDuration(snapshot.getStdDev())),
        timestamp);
    cassandra.send(prefix(name, "p50"),
        format(convertDuration(snapshot.getMedian())),
        timestamp);
    cassandra.send(prefix(name, "p75"),
        format(convertDuration(snapshot.get75thPercentile())),
        timestamp);
    cassandra.send(prefix(name, "p95"),
        format(convertDuration(snapshot.get95thPercentile())),
        timestamp);
    cassandra.send(prefix(name, "p98"),
        format(convertDuration(snapshot.get98thPercentile())),
        timestamp);
    cassandra.send(prefix(name, "p99"),
        format(convertDuration(snapshot.get99thPercentile())),
        timestamp);
    cassandra.send(prefix(name, "p999"),
        format(convertDuration(snapshot.get999thPercentile())),
        timestamp);

    reportMetered(name, timer, timestamp);
  }

  private void reportMetered(String name, Metered meter, Date timestamp) throws DriverException {
    cassandra.send(prefix(name, "count"), format(meter.getCount()), timestamp);
    cassandra.send(prefix(name, "m1_rate"),
        convertRate(meter.getOneMinuteRate()),
        timestamp);
    cassandra.send(prefix(name, "m5_rate"),
        format(convertRate(meter.getFiveMinuteRate())),
        timestamp);
    cassandra.send(prefix(name, "m15_rate"),
        format(convertRate(meter.getFifteenMinuteRate())),
        timestamp);
    cassandra.send(prefix(name, "mean_rate"),
        format(convertRate(meter.getMeanRate())),
        timestamp);
  }

  private void reportHistogram(String name, Histogram histogram, Date timestamp) throws DriverException {
    final Snapshot snapshot = histogram.getSnapshot();
    cassandra.send(prefix(name, "count"), format(histogram.getCount()), timestamp);
    cassandra.send(prefix(name, "max"), format(snapshot.getMax()), timestamp);
    cassandra.send(prefix(name, "mean"), format(snapshot.getMean()), timestamp);
    cassandra.send(prefix(name, "min"), format(snapshot.getMin()), timestamp);
    cassandra.send(prefix(name, "stddev"), format(snapshot.getStdDev()), timestamp);
    cassandra.send(prefix(name, "p50"), format(snapshot.getMedian()), timestamp);
    cassandra.send(prefix(name, "p75"), format(snapshot.get75thPercentile()), timestamp);
    cassandra.send(prefix(name, "p95"), format(snapshot.get95thPercentile()), timestamp);
    cassandra.send(prefix(name, "p98"), format(snapshot.get98thPercentile()), timestamp);
    cassandra.send(prefix(name, "p99"), format(snapshot.get99thPercentile()), timestamp);
    cassandra.send(prefix(name, "p999"), format(snapshot.get999thPercentile()), timestamp);
  }

  private void reportCounter(String name, Counter counter, Date timestamp) throws DriverException {
    cassandra.send(prefix(name, "count"), format(counter.getCount()), timestamp);
  }

  private void reportGauge(String name, Gauge gauge, Date timestamp) throws DriverException {
    final Double value = format(gauge.getValue());
    if (value != null) {
      cassandra.send(prefix(name, "gauge"), value, timestamp);
    }
  }

  private Double format(Object o) {
    if (o instanceof Float) {
      return ((Float) o).doubleValue();
    } else if (o instanceof Double) {
      return (Double)o;
    } else if (o instanceof Byte) {
      return ((Byte) o).doubleValue();
    } else if (o instanceof Short) {
      return ((Short) o).doubleValue();
    } else if (o instanceof Integer) {
      return ((Integer) o).doubleValue();
    } else if (o instanceof Long) {
      return ((Long) o).doubleValue();
    }
    return null;
  }

  private String prefix(String... components) {
    return MetricRegistry.name(prefix, components);
  }

}

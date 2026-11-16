package tech.streamfusion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;

/** Diagnostic counts at operator boundaries, retaining values after task and cluster teardown. */
public final class HudiRecordMetrics implements MetricReporter {
  private static final HudiRecordMetrics SHARED = new HudiRecordMetrics();
  private volatile boolean attached;
  private final Map<String, Reading> live = new HashMap<>();
  private final Map<String, Map<String, Object>> completed = new HashMap<>();

  private HudiRecordMetrics() {}

  public static HudiRecordMetrics shared() {
    return SHARED;
  }

  public void requireAttached() {
    if (!attached) throw new IllegalStateException("Hudi diagnostic reporter is not attached to the test cluster");
  }

  public static final class Factory implements MetricReporterFactory {
    @Override
    public MetricReporter createMetricReporter(Properties properties) {
      SHARED.attached = true;
      return SHARED;
    }
  }

  public synchronized List<Map<String, Object>> snapshot(String job) {
    var values = new java.util.TreeMap<>(completed);
    live.forEach((key, reading) -> values.put(key, reading.snapshot()));
    var selected = new ArrayList<Map<String, Object>>();
    values.values().stream()
        .filter(value -> job.equals(value.get("job_id")))
        .forEach(selected::add);
    return selected;
  }

  public synchronized void dispose() {
    live.clear();
    completed.clear();
  }

  @Override
  public void open(MetricConfig configuration) {}

  // Retain final counts until the benchmark has reported them.
  @Override
  public synchronized void close() {
    live.forEach((key, reading) -> completed.put(key, reading.snapshot()));
    live.clear();
  }

  @Override
  public synchronized void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
    if (!(metric instanceof Counter || metric instanceof Meter)) return;
    if (!(name.equals("numRecordsIn")
        || name.equals("numRecordsOut")
        || name.equals("numOutputRows")
        || name.equals("recordWrittenPerSecond"))) return;
    var variables = group.getAllVariables();
    if (!variables.containsKey("<operator_id>")) return;
    var metadata = new LinkedHashMap<String, Object>();
    for (String variable :
        List.of("job_id", "operator_id", "operator_name", "subtask_index", "task_attempt_id"))
      metadata.put(variable, variables.getOrDefault("<" + variable + ">", ""));
    metadata.put("metric", name);
    metadata.put("kind", metric instanceof Meter ? "cumulative_meter_count" : "counter");
    live.put(key(name, group), new Reading(metric, metadata));
  }

  @Override
  public synchronized void notifyOfRemovedMetric(Metric metric, String name, MetricGroup group) {
    Reading reading = live.remove(key(name, group));
    if (reading != null) completed.put(key(name, group), reading.snapshot());
  }

  private static String key(String name, MetricGroup group) {
    var variables = group.getAllVariables();
    return List.of("job_id", "operator_id", "subtask_index", "task_attempt_id").stream()
            .map(variable -> variables.getOrDefault("<" + variable + ">", ""))
            .collect(java.util.stream.Collectors.joining("/"))
        + "/"
        + group.getMetricIdentifier(name);
  }

  private static final class Reading {
    final Metric metric;
    final Map<String, Object> metadata;

    Reading(Metric metric, Map<String, Object> metadata) {
      this.metric = metric;
      this.metadata = metadata;
    }

    Map<String, Object> snapshot() {
      var value = new LinkedHashMap<>(metadata);
      value.put(
          "count",
          metric instanceof Counter
              ? ((Counter) metric).getCount()
              : ((Meter) metric).getCount());
      return value;
    }
  }
}

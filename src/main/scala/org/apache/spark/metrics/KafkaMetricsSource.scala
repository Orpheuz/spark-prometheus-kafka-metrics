package org.apache.spark.metrics

import com.codahale.metrics.{DefaultSettableGauge, MetricRegistry}
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.KafkaMetricsSource.{CONSUMER_END_OFFSET, CONSUMER_LAST_RECORD_TIMESTAMP, CONSUMER_RECORDS_CONSUMED_RATE, CONSUMER_RECORDS_LAG, CONSUMER_RECORDS_LEAD, CONSUMER_START_OFFSET}
import org.apache.spark.metrics.source.Source

class KafkaMetricsSource extends Source {

  override val sourceName: String = KafkaMetricsSource.sourceName

  override val metricRegistry: MetricRegistry = new MetricRegistry

  def startOffsetGauge(queryName: String, topic: String, partition:String): DefaultSettableGauge[Long] = {
    metricRegistry.gauge(MetricRegistry.name(queryName, topic, partition, CONSUMER_START_OFFSET))
  }

  def endOffsetGauge(queryName: String, topic: String, partition:String): DefaultSettableGauge[Long] = {
    metricRegistry.gauge(MetricRegistry.name(queryName, topic, partition, CONSUMER_END_OFFSET))
  }

  def consumerRecordsLeadGauge(queryName: String, topic: String, partition:String): DefaultSettableGauge[Long] = {
    metricRegistry.gauge(MetricRegistry.name(queryName, topic, partition, CONSUMER_RECORDS_LEAD))
  }

  def consumerRecordsLagGauge(queryName: String, topic: String, partition:String): DefaultSettableGauge[Long] = {
    metricRegistry.gauge(MetricRegistry.name(queryName, topic, partition, CONSUMER_RECORDS_LAG))
  }

  def consumerRecordsConsumedRateGauge(queryName: String): DefaultSettableGauge[Double] = {
    metricRegistry.gauge(MetricRegistry.name(queryName, CONSUMER_RECORDS_CONSUMED_RATE))
  }

  def consumerRecordsTimestampGauge(queryName: String, topic: String, partition:String): DefaultSettableGauge[Long] = {
    metricRegistry.gauge(MetricRegistry.name(queryName, topic, partition, CONSUMER_LAST_RECORD_TIMESTAMP))
  }
}

object KafkaMetricsSource {

  val sourceName = "spark_streaming_kafka"

  val CONSUMER_START_OFFSET: String = "consumer.start.offset"
  val CONSUMER_END_OFFSET: String = "consumer.end.offset"
  val CONSUMER_RECORDS_LEAD: String = "consumer.records.lead"
  val CONSUMER_RECORDS_LAG: String = "consumer.records.lag"
  val CONSUMER_RECORDS_CONSUMED_RATE: String = "consumer.records.consumed.rate"
  val CONSUMER_LAST_RECORD_TIMESTAMP: String = "consumer.records.timestamp"

  def apply(register: Boolean = false): KafkaMetricsSource = {

    SparkEnv.get.metricsSystem.getSourcesByName(sourceName).headOption.getOrElse{
      val source = new KafkaMetricsSource

      if(register) {
        SparkEnv.get.metricsSystem.registerSource(source)
      }

      source
    }.asInstanceOf[KafkaMetricsSource]
  }
}

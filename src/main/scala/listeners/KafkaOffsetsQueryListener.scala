package listeners

import io.circe.parser.decode
import listeners.KafkaOffsetsQueryListener.calculateLag
import org.apache.spark.metrics.KafkaMetricsSource
import org.apache.spark.sql.streaming.StreamingQueryListener

class KafkaOffsetsQueryListener(kafkaMetricsSource: KafkaMetricsSource)
  extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {

    event.progress.sources.filter(_.description.contains("Kafka"))
      .foreach(
        source => {

          // Start offset metrics
          val startOffsets = decode[Map[String, Map[String, Long]]](source.startOffset)

          startOffsets.foreach {
            parsedOffsets =>
              parsedOffsets.foreach {
                case (topic, partitionOffsets) => partitionOffsets.foreach {
                  case (partition, offset) =>
                    kafkaMetricsSource.startOffsetGauge(
                      event.progress.name,
                      topic,
                      partition
                    ).setValue(offset)
                }
              }
          }

          // End offset metrics
          val endOffsets = decode[Map[String, Map[String, Long]]](source.endOffset)

          endOffsets.foreach {
            parsedOffsets =>
              parsedOffsets.foreach {
                case (topic, partitionOffsets) => partitionOffsets.foreach {
                  case (partition, offset) =>
                    kafkaMetricsSource.endOffsetGauge(
                      event.progress.name,
                      topic,
                      partition
                    ).setValue(offset)
                }
              }
          }

          // Latest offset metrics
          val latestOffsets = decode[Map[String, Map[String, Long]]](source.latestOffset)

          latestOffsets.foreach {
            parsedOffsets =>
              parsedOffsets.foreach {
                case (topic, partitionOffsets) => partitionOffsets.foreach {
                  case (partition, offset) =>
                    kafkaMetricsSource.consumerRecordsLeadGauge(
                      event.progress.name,
                      topic,
                      partition
                    ).setValue(offset)
                }
              }
          }

          // Lag metrics
          val lagOffsets = calculateLag(endOffsets.toOption, latestOffsets.toOption)

          lagOffsets.foreach {
            parsedOffsets =>
              parsedOffsets.foreach {
                case (topic, partitionOffsets) => partitionOffsets.foreach {
                  case (partition, offset) =>
                    kafkaMetricsSource.consumerRecordsLagGauge(
                      event.progress.name,
                      topic,
                      partition
                    ).setValue(offset)
                }
              }
          }

          // Consumer rate metrics
          kafkaMetricsSource.consumerRecordsConsumedRateGauge(event.progress.name)
            .setValue(source.inputRowsPerSecond)
        })
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}

object KafkaOffsetsQueryListener {

  def apply(): StreamingQueryListener = {

    new KafkaOffsetsQueryListener(KafkaMetricsSource(register = true))
  }

  def calculateLag(
    endingOffsets: Option[Map[String, Map[String, Long]]],
    latestOffsets: Option[Map[String, Map[String, Long]]]
  ): Option[Map[String, Map[String, Long]]] = {

    for {
      end <- endingOffsets
      latest <- latestOffsets
    } yield {
      latest.map { case (topic, partitionOffset) =>
        (topic, partitionOffset.map {
          case (partition, offset) =>
            (partition, offset - end(topic)(partition))
        })
      }
    }
  }
}
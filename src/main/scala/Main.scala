import expr.KafkaTimestampMetrics
import listeners.KafkaOffsetsQueryListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

object Main {

  def main(args: Array[String]): Unit = {

    val bootstrapServers = args(0)
    val topicName = args(1)

    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("AvroSchemaEvolutionDemo")
      .config("spark.metrics.conf", getClass.getResource("metrics.properties").getPath)
      .config("spark.ui.prometheus.enabled", "true")
      .config("spark.sql.streaming.metricsEnabled", "true")
      .getOrCreate()

    val df = ss
      .readStream
      .format("kafka")
      .option("startingOffsets", "earliest")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("maxOffsetsPerTrigger", "100")
      .option("subscribe", s"$topicName")
      .load()

    val queryName = "NoopKafkaQuery"

    df.withColumn("value", KafkaTimestampMetrics.metrics(queryName, struct(df.columns.map(col): _*)))
      .writeStream
      .format("noop")
      .queryName(queryName)
      .start()

    ss.streams.addListener(KafkaOffsetsQueryListener())

    ss.streams.awaitAnyTermination()
  }
}
package expr

import org.apache.spark.metrics.KafkaMetricsSource
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType}

case class KafkaTimestampMetrics(queryName: String, child: Expression)
  extends UnaryExpression
  with NullIntolerant with CodegenFallback {

  @transient private val kafkaMetricsSource = KafkaMetricsSource()

  override def dataType: DataType = BinaryType

  override def eval(input: InternalRow): Any = {

    val topic = input.getString(2)
    val partition = input.getInt(3)
    val timestamp = input.getLong(5) / 1000

    kafkaMetricsSource
      .consumerRecordsTimestampGauge(queryName, topic, partition.toString)
      .setValue(timestamp)

    input.get(1, BinaryType)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {

    this.copy(child = newChild)
  }
}

object KafkaTimestampMetrics {

  def metrics(queryName: String, col: Column): Column = {

    new Column(KafkaTimestampMetrics(queryName, col.expr))
  }
}


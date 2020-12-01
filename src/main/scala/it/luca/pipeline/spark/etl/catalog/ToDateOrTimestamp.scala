package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.SingleColumnExpression
import it.luca.pipeline.spark.etl.parsing.EtlExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_date, to_timestamp}

case class ToDateOrTimestamp(override val expression: String)
  extends SingleColumnExpression(expression, EtlExpression.ToDateOrTimestamp) {

  private val format: String = group(3)

  override def getColumn(inputColumn: Column): Column = {

    val f: (Column, String) => Column = if (functionName endsWith "date") to_date else to_timestamp
    f(inputColumn, format)
  }

  override def asString: String = s"${functionName.toUpperCase}($nestedFunction, FORMAT = $format)"
}

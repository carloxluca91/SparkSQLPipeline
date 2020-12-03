package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.SingleColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_date, to_timestamp}

case class ToDateOrTimestamp(override val expression: String)
  extends SingleColumnExpression(expression, Catalog.ToDateOrTimestamp) {

  private val format: String = group(3)
  override protected val transformationFunction: Column => Column = {
    val timeFunction: (Column, String) => Column = if (functionName.toLowerCase endsWith "date") to_date else to_timestamp
    timeFunction(_, format)
  }

  override protected def asString: String = s"$functionName($nestedFunction, format = '$format')"
}

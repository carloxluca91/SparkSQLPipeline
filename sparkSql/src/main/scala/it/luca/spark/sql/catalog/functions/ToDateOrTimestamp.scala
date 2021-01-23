package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_date, to_timestamp}

case class ToDateOrTimestamp(override val expression: String)
  extends SingleColumnSqlFunction(expression, SqlCatalog.ToDateOrTimestamp) {

  private val format: String = group(3)

  override protected val transformationFunction: Column => Column = {
    val timeFunction: (Column, String) => Column = if (functionName.toLowerCase endsWith "date") to_date else to_timestamp
    timeFunction(_, format)
  }

  override protected def asString: String = s"$functionName($nestedFunction, format = '$format')"
}

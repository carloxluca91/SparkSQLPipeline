package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSQLFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.date_format

case class DateFormat(override val expression: String)
  extends SingleColumnSQLFunction(expression, SQLCatalog.DateFormat) {

  private val format: String = group(3)

  override protected val transformationFunction: Column => Column = date_format(_, format)

  override protected def asString: String = s"$functionName($nestedFunction, format = $format)"
}

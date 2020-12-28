package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSQLFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.substring

case class Substring(override val expression: String)
  extends SingleColumnSQLFunction(expression, SQLCatalog.Substring) {

  private val start: Int = group(3).toInt
  private val length: Int = group(4).toInt

  override protected val transformationFunction: Column => Column = substring(_, start, length)

  override protected def asString: String = s"$functionName($nestedFunction, start = $start, length = $length)"
}

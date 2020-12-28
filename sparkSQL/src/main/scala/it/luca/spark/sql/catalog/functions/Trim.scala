package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSQLFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.trim

case class Trim(override val expression: String)
  extends SingleColumnSQLFunction(expression, SQLCatalog.Trim) {

  override protected val transformationFunction: Column => Column = trim

  override protected def asString: String = s"$functionName($nestedFunction)"
}

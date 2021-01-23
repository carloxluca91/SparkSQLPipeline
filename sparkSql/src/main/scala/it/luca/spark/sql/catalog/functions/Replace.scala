package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.regexp_replace

case class Replace(override val expression: String)
  extends SingleColumnSqlFunction(expression, SqlCatalog.Replace) {

  final val pattern: String = group(3)
  final val replacement: String = group(4)

  override protected val transformationFunction: Column => Column = regexp_replace(_, pattern, replacement)

  override protected def asString: String = s"$functionName($nestedFunction, pattern = '$pattern', replacement = '$replacement')"
}

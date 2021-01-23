package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lower, upper}

case class LowerOrUpper(override val expression: String)
  extends SingleColumnSqlFunction(expression, SqlCatalog.LowerOrUpper) {

  override protected val transformationFunction: Column => Column = {
    if (functionName.toLowerCase startsWith "l") lower else upper
  }

  override protected def asString: String = s"$functionName($nestedFunction)"
}

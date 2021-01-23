package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lpad, rpad}

case class LeftOrRightPad(override val expression: String)
  extends SingleColumnSqlFunction(expression, SqlCatalog.LeftOrRightPad) {

  final val length: Int = group(3).toInt
  final val padding: String = group(4)

  override protected val transformationFunction: Column => Column = {

    val paddingFunction: (Column, Int, String) => Column =
      if (functionName.toLowerCase startsWith "l") lpad else rpad

    paddingFunction(_, length, padding)
  }

  override protected def asString: String = s"$functionName($nestedFunction, length = $length, padding = '$padding')"
}


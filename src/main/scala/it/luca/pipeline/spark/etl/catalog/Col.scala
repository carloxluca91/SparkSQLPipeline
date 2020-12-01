package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.StaticColumnExpression
import it.luca.pipeline.spark.etl.parsing.EtlExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class Col(override val expression: String)
  extends StaticColumnExpression(expression, EtlExpression.Col) {

  val columnName: String = group(2)

  override def getColumn: Column = col(columnName)

  override def asString: String = s"${functionName.toUpperCase}($columnName)"
}

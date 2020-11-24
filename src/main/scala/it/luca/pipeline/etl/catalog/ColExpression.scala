package it.luca.pipeline.etl.catalog

import it.luca.pipeline.etl.common.StaticColumnExpression
import it.luca.pipeline.etl.parsing.EtlExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class ColExpression(override val expression: String)
  extends StaticColumnExpression(expression, EtlExpression.Col) {

  private final val columnName: String = group(2)

  override def getColumn: Column = col(columnName)

  override def asString: String = s"${functionName.toUpperCase}($columnName)"
}

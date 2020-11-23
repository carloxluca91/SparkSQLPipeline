package it.luca.pipeline.etl.function

import it.luca.pipeline.etl.common.StaticExpression
import it.luca.pipeline.etl.expression.EtlExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class ColExpression(override val expression: String)
  extends StaticExpression(expression, EtlExpression.Col) {

  private final val columnName: String = group(2)

  override def getColumn: Column = col(columnName)

  override def asString: String = s"${functionName.toUpperCase}($columnName)"
}

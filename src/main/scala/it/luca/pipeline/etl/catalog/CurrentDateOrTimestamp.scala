package it.luca.pipeline.etl.catalog

import it.luca.pipeline.etl.common.StaticColumnExpression
import it.luca.pipeline.etl.parsing.EtlExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{current_date, current_timestamp}

case class CurrentDateOrTimestamp(override val expression: String)
  extends StaticColumnExpression(expression, EtlExpression.CurrentDateOrTimestamp) {

  override def getColumn: Column = if (functionName endsWith "date") current_date else current_timestamp

  override def asString: String = s"${functionName.toUpperCase}()"
}

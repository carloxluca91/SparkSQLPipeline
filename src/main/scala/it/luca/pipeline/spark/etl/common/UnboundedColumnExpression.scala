package it.luca.pipeline.spark.etl.common

import it.luca.pipeline.spark.etl.parsing.EtlExpression
import org.apache.spark.sql.Column

abstract class UnboundedColumnExpression(override val expression: String,
                                         override val etlExpression: EtlExpression.Value)
  extends MultipleColumnExpression(expression, etlExpression) {

  def getColumn(inputColumns: Column*): Column

}

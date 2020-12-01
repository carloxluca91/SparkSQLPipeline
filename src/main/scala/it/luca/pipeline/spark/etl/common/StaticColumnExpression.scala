package it.luca.pipeline.spark.etl.common
import it.luca.pipeline.spark.etl.parsing.EtlExpression
import org.apache.spark.sql.Column

abstract class StaticColumnExpression(override val expression: String,
                                      override val etlExpression: EtlExpression.Value)
  extends AbstractExpression(expression, etlExpression) {

  def getColumn: Column

}

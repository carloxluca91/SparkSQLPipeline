package it.luca.pipeline.etl.common
import it.luca.pipeline.etl.expression.EtlExpression
import org.apache.spark.sql.Column

abstract class StaticExpression(override val expression: String,
                                override val etlExpression: EtlExpression.Value)
  extends AbstractExpression(expression, etlExpression) {

  def getColumn: Column

}

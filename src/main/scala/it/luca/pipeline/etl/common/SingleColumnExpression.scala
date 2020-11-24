package it.luca.pipeline.etl.common
import it.luca.pipeline.etl.parsing.EtlExpression
import org.apache.spark.sql.Column

abstract class SingleColumnExpression(override val expression: String,
                                      override val etlExpression: EtlExpression.Value)
  extends AbstractExpression(expression, etlExpression) {

  final val nestedFunction: String = group(2)

  def getColumn(inputColumn: Column): Column

}
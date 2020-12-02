package it.luca.pipeline.spark.etl.common
import it.luca.pipeline.spark.etl.catalog.EtlExpression
import org.apache.spark.sql.Column

abstract class TwoColumnExpression(override val expression: String,
                                   override val etlExpression: EtlExpression.Value)
  extends AbstractExpression(expression, etlExpression) {

  val firstExpression: String = group(2)
  val secondExpression: String = group(3)

  def getColumn(firstColumn: Column, secondColumn: Column): Column
}

package it.luca.pipeline.spark.etl.common
import it.luca.pipeline.spark.etl.parsing.EtlExpression
import org.apache.spark.sql.Column

abstract class TwoColumnExpression(override val expression: String,
                                   override val etlExpression: EtlExpression.Value)
  extends MultipleColumnExpression(expression, etlExpression) {

  if (subExpressions.size > 2) {
    throw new IllegalArgumentException(s"More than two subexpressions identified (${subExpressions.mkString(", ")})")
  }

  val firstExpression: String = subExpressions.head
  val secondExpression: String = subExpressions(1)

  def getColumn(firstColumn: Column, secondColumn: Column): Column
}

package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.TwoColumnExpression
import org.apache.spark.sql.Column

case class IsEqualOrIsNotEqual(override val expression: String)
  extends TwoColumnExpression(expression, EtlExpression.IsEqualOrIsNotEqual) {

  private val isANotEqual = functionName.toLowerCase contains "not"

  override def getColumn(firstColumn: Column, secondColumn: Column): Column = {

    if (isANotEqual) firstColumn =!= secondColumn else firstColumn === secondColumn
  }

  override def asString: String = s"$firstExpression.${if (isANotEqual) "isNotEqual" else "isEqual"}($secondExpression)"
}

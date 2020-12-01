package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.TwoColumnExpression
import it.luca.pipeline.spark.etl.parsing.EtlExpression
import org.apache.spark.sql.Column

case class EqualToOrNotEqualTo(override val expression: String)
  extends TwoColumnExpression(expression, EtlExpression.EqualToOrNotEqualTo) {

  private val isANotEqualTo = functionName.toLowerCase startsWith "not"

  override def getColumn(firstColumn: Column, secondColumn: Column): Column = {

    if (isANotEqualTo) firstColumn =!= secondColumn else firstColumn === secondColumn
  }

  override def asString: String = s"$firstExpression ${if (isANotEqualTo) "=" else "<>"} $secondExpression"
}

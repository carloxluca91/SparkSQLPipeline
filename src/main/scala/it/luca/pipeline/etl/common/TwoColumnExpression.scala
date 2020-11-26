package it.luca.pipeline.etl.common
import it.luca.pipeline.etl.parsing.EtlExpression

abstract class TwoColumnExpression(override val expression: String,
                                   override val etlExpression: EtlExpression.Value)
  extends MultipleColumnExpression(expression, etlExpression) {

  if (subExpressions.size > 2) {
    throw new IllegalArgumentException(s"More than two subexpressions identified (${subExpressions.mkString(", ")})")
  }
}

package it.luca.pipeline.exception

import it.luca.pipeline.etl.parsing.EtlExpression

case class UnmatchedExpressionException(etlExpression: String)
  extends Throwable(s"Unable to match following expression ($etlExpression) " +
    s"to any of the values within ${EtlExpression.getClass.getSimpleName} enumeration")

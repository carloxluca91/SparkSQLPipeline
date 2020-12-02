package it.luca.pipeline.exception

import it.luca.pipeline.spark.etl.catalog.EtlExpression

case class UndefinedCatalogExpression(etlExpression: EtlExpression.Value)
  extends Throwable(s"Unable to match following expression (${etlExpression.regex.toString()}) to any catalog expression")

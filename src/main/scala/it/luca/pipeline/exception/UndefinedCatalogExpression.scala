package it.luca.pipeline.exception

import it.luca.pipeline.spark.etl.catalog.Catalog

case class UndefinedCatalogExpression(etlExpression: Catalog.Value)
  extends Throwable(s"Unable to match following expression (${etlExpression.regex.toString()}) to any catalog expression")

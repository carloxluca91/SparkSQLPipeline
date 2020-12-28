package it.luca.spark.sql.catalog.exceptions

import it.luca.spark.sql.catalog.functions.SQLCatalog

case class UnmatchedSQLCatalogCaseException(string: String, catalogValue: SQLCatalog.Value)
  extends Throwable(s"SQL function <$string> matches with $catalogValue. " +
    s"However, this latter is not associated to any SQL function")

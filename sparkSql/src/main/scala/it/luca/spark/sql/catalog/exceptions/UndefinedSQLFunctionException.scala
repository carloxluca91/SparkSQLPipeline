package it.luca.spark.sql.catalog.exceptions

case class UndefinedSQLFunctionException(string: String)
  extends Throwable(string)

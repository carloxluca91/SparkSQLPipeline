package it.luca.spark.sql.extensions

import org.apache.spark.sql.DataFrameWriter

class DataFrameWriterExtensions(private val dataFrameWriter: DataFrameWriter[_]) {

  def option(key: String, valueOpt: Option[String]): DataFrameWriter[_] = {

    valueOpt
      .map(s => dataFrameWriter.option(key, s))
      .getOrElse(dataFrameWriter)
  }

  def partitionBy(colNamesOpt: Option[Seq[String]]): DataFrameWriter[_] = {

    colNamesOpt
      .map(s => dataFrameWriter.partitionBy(s: _*))
      .getOrElse(dataFrameWriter)
  }
}

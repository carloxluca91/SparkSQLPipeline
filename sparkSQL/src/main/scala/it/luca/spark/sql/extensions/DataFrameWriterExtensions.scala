package it.luca.spark.sql.extensions

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrameWriter

class DataFrameWriterExtensions(private val dataFrameWriter: DataFrameWriter[_]) {

  private val log = Logger.getLogger(getClass)

  def path(pathOpt: Option[String]): DataFrameWriter[_] = {

    if (pathOpt.nonEmpty) {

      val path = pathOpt.get
      log.info(s"Provided output HDFS path: '$path'")
      dataFrameWriter.option("path", path)

    } else dataFrameWriter
  }

  def partitionBy(colNamesOpt: Option[Seq[String]]): DataFrameWriter[_] = {

    val (partitionByInfo, dataframeWriteMaybePartitioned): (String, DataFrameWriter[_]) = colNamesOpt match {
      case None => ("Partitioning columns not specified", dataFrameWriter)
      case Some(x) =>
        val msg = s"Setting partitioning columns to [${x.map(y => s"'$y'").mkString(", ")}] (using .partitionBy)"
        (msg, dataFrameWriter.partitionBy(x: _*))
    }

    log.info(s"$partitionByInfo")
    dataframeWriteMaybePartitioned
  }
}

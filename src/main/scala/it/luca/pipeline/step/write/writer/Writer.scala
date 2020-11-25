package it.luca.pipeline.step.write.writer

import it.luca.pipeline.step.write.option.WriteOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

trait Writer[T <: WriteOptions] {

  private final val logger = Logger.getLogger(getClass)

  final def dataFrameWriter(dataFrame: DataFrame, writeOptions: T): DataFrameWriter[Row] = {

    val (coalesceInfoStr, dataFrameMaybeCoalesced): (String, DataFrame) = writeOptions.coalesce match {
      case None => ("Coalesce option unset", dataFrame)
      case Some(x) => (s"Coalesce option = $x", dataFrame.coalesce(x))
    }

    val (partitionByInfoStr, dataframeWriteMaybePartitioned): (String, DataFrameWriter[Row]) = writeOptions.partitionBy match {
      case None => ("partitionBy options unset", dataFrameMaybeCoalesced.write)
      case Some(x) => (s"partitionBy option = ${x.map(y => s"'$y'").mkString(", ")}", dataFrameMaybeCoalesced.write.partitionBy(x: _*))
    }

    logger.info(s"$coalesceInfoStr, $partitionByInfoStr")
    dataframeWriteMaybePartitioned
  }

  def write(dataFrame: DataFrame, writeOptions: T, sparkSession: SparkSession): Unit

}

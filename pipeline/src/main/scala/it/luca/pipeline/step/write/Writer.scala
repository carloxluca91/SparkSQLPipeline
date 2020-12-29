package it.luca.pipeline.step.write

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

trait Writer[T <: WriteOptions] {

  private val log = Logger.getLogger(getClass)

  private def setUpWriter(dataFrame: DataFrame, saveOptions: SaveOptions): DataFrameWriter[Row] = {

    val (coalesceInfoStr, dataFrameMaybeCoalesced): (String, DataFrame) = saveOptions.coalesce match {
      case None => ("Coalesce option unset", dataFrame)
      case Some(x) => (s"Coalesce option = $x", dataFrame.coalesce(x))
    }

    val (partitionByInfoStr, dataframeWriteMaybePartitioned): (String, DataFrameWriter[Row]) = saveOptions.partitionBy match {
      case None => ("partitionBy options unset", dataFrameMaybeCoalesced.write)
      case Some(x) => (s"partitionBy option = ${x.map(y => s"'$y'").mkString(", ")}", dataFrameMaybeCoalesced.write.partitionBy(x: _*))
    }

    log.info(s"$coalesceInfoStr, $partitionByInfoStr")
    dataframeWriteMaybePartitioned
  }

  protected def writeDataFrame(dataFrameWriter: DataFrameWriter[Row], writeOptions: T): Unit

  def write(dataFrame: DataFrame, writeOptions: T): Unit = {

    val dataFrameWriter: DataFrameWriter[Row] = setUpWriter(dataFrame, writeOptions.saveOptions)
    writeDataFrame(dataFrameWriter, writeOptions)
  }
}

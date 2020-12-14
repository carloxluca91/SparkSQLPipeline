package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.read.option.common.ReadOptions
import it.luca.pipeline.step.read.option.concrete.{ReadCsvOptions, ReadHiveTableOptions}
import it.luca.pipeline.step.read.reader.concrete.{CsvReader, HiveTableReader}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ReadStep(override val name: String,
                    override val description: String,
                    override val stepType: String,
                    override val outputDfId: String,
                    srcOptions: ReadOptions)
  extends AbstractStep(name, description, stepType, outputDfId) {

  private final val logger = Logger.getLogger(classOf[ReadStep])

  def read(sparkSession: SparkSession): DataFrame = {

    val readDataframe: DataFrame = srcOptions match {
      case csvSrcOptions: ReadCsvOptions => CsvReader.read(csvSrcOptions, sparkSession)
      case hiveTableSrcOptions: ReadHiveTableOptions => HiveTableReader.read(hiveTableSrcOptions, sparkSession)
    }

    logger.info(s"Successfully read dataframe '$outputDfId' during readStep $name")
    readDataframe
  }
}

object ReadStep {

  implicit def decodeJson: DecodeJson[ReadStep] = DecodeJson.derive[ReadStep]
}

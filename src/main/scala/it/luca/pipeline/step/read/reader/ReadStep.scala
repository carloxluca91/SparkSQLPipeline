package it.luca.pipeline.step.read.reader

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.read.option.{CsvSrcOptions, HiveTableSrcOptions, SrcOptions}
import it.luca.pipeline.utils.JobProperties
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ReadStep(override val name: String,
                    override val description: String,
                    override val stepType: String,
                    override val dataframeId: String,
                    srcOptions: SrcOptions)
  extends AbstractStep(name, description, stepType, dataframeId) {

  private final val logger = Logger.getLogger(classOf[ReadStep])

  def read(sparkSession: SparkSession, jobProperties: JobProperties): DataFrame = {

    val readDataframe: DataFrame = srcOptions match {
      case csvSrcOptions: CsvSrcOptions => CsvReader.read(csvSrcOptions, sparkSession, jobProperties)
      case hiveTableSrcOptions: HiveTableSrcOptions => HiveTableReader.read(hiveTableSrcOptions, sparkSession, jobProperties)
    }

    logger.info(s"Successfully read dataframe '$dataframeId' of step '$name'")
    readDataframe
  }
}

object ReadStep {

  implicit def decodeJson: DecodeJson[ReadStep] = DecodeJson.derive[ReadStep]
}

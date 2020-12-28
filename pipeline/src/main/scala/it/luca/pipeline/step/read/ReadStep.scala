package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ReadStep(override val name: String,
                    override val description: String,
                    override val stepType: String,
                    outputAlias: String,
                    readOptions: ReadOptions)
  extends AbstractStep(name, description, stepType, outputAlias) {

  private val log = Logger.getLogger(classOf[ReadStep])

  def read(sparkSession: SparkSession): DataFrame = {

    val readDataframe: DataFrame = readOptions match {
      case csv: ReadCsvOptions => CsvReader.read(csv, sparkSession)
      case hive: ReadHiveTableOptions => HiveTableReader.read(hive, sparkSession)
    }

    log.info(s"Successfully read dataframe '$outputAlias' during readStep '$name'")
    readDataframe
  }
}

object ReadStep {

  implicit def decodeJson: DecodeJson[ReadStep] = DecodeJson.derive[ReadStep]
}

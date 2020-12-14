package it.luca.pipeline.step.write

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.write.option.common.{WriteFileOptions, WriteOptions, WriteTableOptions}
import it.luca.pipeline.step.write.option.concrete._
import it.luca.pipeline.step.write.writer.concrete._
import it.luca.spark.sql.utils.DataFrameUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

case class WriteStep(override val name: String,
                     override val description: String,
                     override val stepType: String,
                     override val outputDfId: String,
                     writeOptions: WriteOptions)
  extends AbstractStep(name, description, stepType, outputDfId) {

  private val logger = Logger.getLogger(getClass)

  def write(dataFrame: DataFrame): Unit = {

    logger.info(s"DataFrame to be written ('$outputDfId') has schema: ${DataFrameUtils.dataframeSchema(dataFrame)}")

    writeOptions match {
      case _: WriteFileOptions =>
      case tableOptions: WriteTableOptions => tableOptions match {
        case wht: WriteHiveTableOptions => HiveTableWriter.write(dataFrame, wht)
        case wjt: WriteJDBCTableOptions => JDBCTableWriter.write(dataFrame, wjt)
      }
    }

    logger.info(s"Successfully written dataframe '$outputDfId' during writeStep $name")
  }
}

object WriteStep {

  implicit def decodeJson: DecodeJson[WriteStep] = DecodeJson.derive[WriteStep]
}

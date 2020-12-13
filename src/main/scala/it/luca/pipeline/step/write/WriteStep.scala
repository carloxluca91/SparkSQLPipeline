package it.luca.pipeline.step.write

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.write.common.{WriteFileOptions, WriteOptions, WriteTableOptions}
import it.luca.pipeline.step.write.option._
import it.luca.pipeline.step.write.writer.{HiveTableWriter, JDBCTableWriter}
import it.luca.spark.sql.utils.DataFrameUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

case class WriteStep(override val name: String,
                     override val description: String,
                     override val stepType: String,
                     override val dataframeId: String,
                     writeOptions: WriteOptions)
  extends AbstractStep(name, description, stepType, dataframeId) {

  private val logger = Logger.getLogger(getClass)

  def write(dataFrame: DataFrame): Unit = {

    logger.info(s"DataFrame to be written ('$dataframeId') has schema: ${DataFrameUtils.dataframeSchema(dataFrame)}")

    writeOptions match {
      case _: WriteFileOptions =>
      case tableOptions: WriteTableOptions => tableOptions match {
        case wht: WriteHiveTableOptions => HiveTableWriter.write(dataFrame, wht)
        case wjt: WriteJDBCTableOptions => JDBCTableWriter.write(dataFrame, wjt)
      }
    }

    logger.info(s"Successfully written dataframe '$dataframeId' during writeStep $name")
  }
}

object WriteStep {

  implicit def decodeJson: DecodeJson[WriteStep] = DecodeJson.derive[WriteStep]
}

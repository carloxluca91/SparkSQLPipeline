package it.luca.pipeline.step.write

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.spark.sql.extensions._
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

case class WriteStep(override val name: String, override val description: String, override val stepType: String,
                     inputAlias: String, writeOptions: WriteOptions)
  extends AbstractStep(name, description, stepType, inputAlias) {

  private val log = Logger.getLogger(getClass)

  def write(dataFrame: DataFrame): Unit = {

    log.info(s"Starting to write dataFrame '$inputAlias'. Schema: ${dataFrame.prettySchema}")
    writeOptions match {
      case _: WriteFileOptions =>
      case table: WriteTableOptions => table match {
        case h: WriteHiveTableOptions => HiveTableWriter.write(dataFrame, h)
      }
    }

    log.info(s"Successfully written dataframe '$inputAlias' during writeStep $name")
  }
}

object WriteStep {

  implicit def decodeJson: DecodeJson[WriteStep] = DecodeJson.derive[WriteStep]
}

package it.luca.pipeline.step.write.writer

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.write.option._
import org.apache.spark.sql.DataFrame

case class WriteStep(override val name: String,
                     override val description: String,
                     override val stepType: String,
                     override val dataframeId: String,
                     writeOptions: WriteOptions)
  extends AbstractStep(name, description, stepType, dataframeId) {

  def write(dataFrame: DataFrame): Unit = {

    writeOptions match {
      case _: WriteFileOptions =>
      case options: WriteTableOptions => options match {
        case hiveTableOptions: WriteHiveTableOptions => HiveTableWriter.write(dataFrame, hiveTableOptions)
        case jdbcTableOptions: WriteJDBCTableOptions => JDBCTableWriter.write(dataFrame, jdbcTableOptions)
      }
    }
  }
}

object WriteStep {

  implicit def decodeJson: DecodeJson[WriteStep] = DecodeJson.derive[WriteStep]
}

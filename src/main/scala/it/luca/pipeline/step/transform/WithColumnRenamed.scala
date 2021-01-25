package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object WithColumnRenamed extends SingleDfTransformation[WithColumnRenamedOptions] {

  private val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: WithColumnRenamedOptions, dataFrame: DataFrame): DataFrame = {

    val columnsToRename: Seq[WithColumnRenamedOption] = transformationOptions.columns
    val columnsToRenameStr = columnsToRename.map(x => s"   ${x.oldName} --> ${x.newName}").mkString(",\n")
    logger.info(s"Detected ${columnsToRename.size} column(s) to rename.\n\n$columnsToRenameStr\n")

    val outputDf: DataFrame = columnsToRename
      .foldLeft(dataFrame)((df, t) => {
        df.withColumnRenamed(t.oldName, t.newName)
      })

    logger.info(s"Successfully renamed all of ${columnsToRename.size} column(s)")
    outputDf
  }
}

case class WithColumnRenamedOptions(override val transformationType: String, override val transformationOrder: Int, columns: List[WithColumnRenamedOption])
  extends TransformationOptions(transformationType, transformationOrder)

object WithColumnRenamedOptions {

  implicit def decodeJson: DecodeJson[WithColumnRenamedOptions] = DecodeJson.derive[WithColumnRenamedOptions]
}

case class WithColumnRenamedOption(oldName: String, newName: String)

object WithColumnRenamedOption {

  implicit def decodeJson: DecodeJson[WithColumnRenamedOption] = DecodeJson.derive[WithColumnRenamedOption]
}

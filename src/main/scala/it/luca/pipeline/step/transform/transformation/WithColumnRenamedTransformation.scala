package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.step.transform.common.SingleSrcTransformation
import it.luca.pipeline.step.transform.option.{WithColumnRenamedOption, WithColumnRenamedTransformationOptions}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object WithColumnRenamedTransformation extends SingleSrcTransformation[WithColumnRenamedTransformationOptions] {

  private val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: WithColumnRenamedTransformationOptions, dataFrame: DataFrame): DataFrame = {

    val columnsToRename: Seq[WithColumnRenamedOption] = transformationOptions.columns
    val columnsToRenameStr = columnsToRename.map(x => s"   ${x.oldName} --> ${x.newName}").mkString(",\n")
    logger.info(s"Detected ${columnsToRename.size} column(s) to rename.\n\n$columnsToRenameStr\n")

    val outputDf: DataFrame = columnsToRename
      .foldLeft(dataFrame)((df, t) => {
        df.withColumnRenamed(t.oldName, t.newName)})

    logger.info(s"Successfully renamed all of ${columnsToRename.size} column(s)")
    outputDf
  }
}

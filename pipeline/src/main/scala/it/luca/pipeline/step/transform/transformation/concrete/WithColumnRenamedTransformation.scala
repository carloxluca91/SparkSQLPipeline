package it.luca.pipeline.step.transform.transformation.concrete

import it.luca.pipeline.step.transform.{WithColumnRenamedOption, WithColumnRenamedOptions}
import it.luca.pipeline.step.transform.option.concrete.WithColumnRenamedOption
import it.luca.pipeline.step.transform.transformation.common.SingleSrcTransformation
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object WithColumnRenamedTransformation extends SingleSrcTransformation[WithColumnRenamedOptions] {

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

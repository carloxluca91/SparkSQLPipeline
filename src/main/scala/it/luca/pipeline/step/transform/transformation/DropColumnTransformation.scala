package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.step.transform.option.DropColumnTransformationOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object DropColumnTransformation extends SingleSrcTransformation[DropColumnTransformationOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: DropColumnTransformationOptions, dataFrame: DataFrame): DataFrame = {

    val numberOfColumns = transformationOptions.columns.size
    val dataframeId = transformationOptions.inputSourceId
    logger.info(s"Identified $numberOfColumns column(s) to drop from dataframe '$dataframeId'")
    transformationOptions.columns
      .foldLeft(dataFrame)((df, columnName) => {
        if (!df.columns.contains(columnName)) {
          logger.warn(s"Column '$columnName' is not defined on dataframe '$dataframeId'")
        }

        df.drop(columnName)
      })
  }
}

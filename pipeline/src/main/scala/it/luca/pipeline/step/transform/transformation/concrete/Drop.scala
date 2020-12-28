package it.luca.pipeline.step.transform.transformation.concrete

import it.luca.pipeline.step.transform.DropColumnTransformationOptions
import it.luca.pipeline.step.transform.transformation.Transformation
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object Drop extends Transformation[DropColumnTransformationOptions] {

  private final val log = Logger.getLogger(getClass)

  override def transform(transformationOptions: DropColumnTransformationOptions, dataFrame: DataFrame): DataFrame = {

    val numberOfColumns = transformationOptions.columns.size
    log.info(s"Identified $numberOfColumns column(s) to drop")
    transformationOptions.columns
      .foldLeft(dataFrame)((df, columnName) => {
        if (!df.columns.contains(columnName)) {
          log.warn(s"Column '$columnName' is not defined. Thus, this will result in a no-op")
        }

        df.drop(columnName)
      })
  }
}

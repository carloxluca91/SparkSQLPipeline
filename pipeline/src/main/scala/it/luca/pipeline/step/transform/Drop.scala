package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object Drop extends SingleDfTransformation[DropOptions] {

  private final val log = Logger.getLogger(getClass)

  override def transform(transformationOptions: DropOptions, dataFrame: DataFrame): DataFrame = {

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


case class DropOptions(override val transformationType: String, override val transformationOrder: Int, columns: List[String])
  extends TransformationOptions(transformationType, transformationOrder)

object DropOptions {

  implicit def decodeJson: DecodeJson[DropOptions] = DecodeJson.derive[DropOptions]
}
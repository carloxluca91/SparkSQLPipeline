package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import it.luca.spark.sql.catalog.parser.SQLFunctionParser
import org.apache.spark.sql.{Column, DataFrame}

object Select extends SingleDfTransformation[SelectOptions] {

  override def transform(transformationOptions: SelectOptions, dataFrame: DataFrame): DataFrame = {

    val selectColumns: Seq[Column] = parseSQLColumns[String, Column](transformationOptions.columns,
      SQLFunctionParser.parse, identity, "column to select")
    dataFrame.select(selectColumns: _*)
  }
}

case class SelectOptions(override val transformationType: String, override val transformationOrder: Int, columns: List[String])
  extends TransformationOptions(transformationType, transformationOrder)

object SelectOptions {

  implicit def decodeJson: DecodeJson[SelectOptions] = DecodeJson.derive[SelectOptions]
}

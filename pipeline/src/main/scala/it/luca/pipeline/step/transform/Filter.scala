package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import it.luca.spark.sql.catalog.parser.SqlFunctionParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object Filter extends SingleDfTransformation[FilterOptions] {

  private val log = Logger.getLogger(getClass)

  override def transform(transformationOptions: FilterOptions, dataFrame: DataFrame): DataFrame = {

    val filterConditionStr: String = transformationOptions.filterCondition
    val filterConditionColumn: Column = SqlFunctionParser.parse(filterConditionStr)

    log.info(s"Successfully parsed filter condition '$filterConditionStr'")
    dataFrame.filter(filterConditionColumn)
  }
}


case class FilterOptions(override val transformationType: String, override val transformationOrder: Int, filterCondition: String)
  extends TransformationOptions(transformationType, transformationOrder)

object FilterOptions {

  implicit def decodeJson: DecodeJson[FilterOptions] = DecodeJson.derive[FilterOptions]
}

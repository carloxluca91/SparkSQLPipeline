package it.luca.pipeline.step.transform.transformation.concrete

import it.luca.pipeline.step.transform.SelectOptions
import it.luca.pipeline.step.transform.transformation.Transformation
import it.luca.spark.sql.catalog.parser.SQLFunctionParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object Select extends Transformation[SelectOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: SelectOptions, dataFrame: DataFrame): DataFrame = {

    val (numberOfColumns, dataframeId): (Int, String) = (transformationOptions.columns.size, transformationOptions.inputDfId)
    logger.info(s"Identified $numberOfColumns column(s) to select on dataframe '$dataframeId'. Trying to parse each of these")
    val selectColumns: Seq[Column] = transformationOptions
      .columns
      .map(SQLFunctionParser.parse)

    logger.info(s"Successfully parsed each of the $numberOfColumns column(s) to select on dataframe '$dataframeId'")
    dataFrame.select(selectColumns: _*)
  }
}

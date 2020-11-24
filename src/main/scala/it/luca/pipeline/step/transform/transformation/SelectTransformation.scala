package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.etl.parsing.EtlExpressionParser
import it.luca.pipeline.step.transform.option.SelectTransformationOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object SelectTransformation extends SingleSrcTransformation[SelectTransformationOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: SelectTransformationOptions, dataFrame: DataFrame): DataFrame = {

    val (numberOfColumns, dataframeId): (Int, String) = (transformationOptions.columns.size, transformationOptions.inputSourceId)
    logger.info(s"Identified $numberOfColumns column(s) to select for dataframe '$dataframeId'. Trying to parse each of these")
    val selectColumns: Seq[Column] = transformationOptions
      .columns
      .map(EtlExpressionParser.parse)

    logger.info(s"Successfully parsed each of the $numberOfColumns column(s) to select for dataframe '$dataframeId'")
    dataFrame.select(selectColumns: _*)
  }
}
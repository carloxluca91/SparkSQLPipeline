package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import it.luca.spark.sql.catalog.parser.SQLFunctionParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object WithColumn extends SingleDfTransformation[WithColumnOptions] {

  private val log = Logger.getLogger(getClass)

  override def transform(transformationOptions: WithColumnOptions, dataFrame: DataFrame): DataFrame = {

    val columnsToAdd: Seq[(String, Column)] = parseSQLColumns[ColumnOption, (String, Column)](transformationOptions.columns,
      c => (c.alias, SQLFunctionParser.parse(c.expression)),
      c => s"${c.expression}.as(${c.alias})",
      "column to add")

    columnsToAdd
      .foldLeft(dataFrame)((df, tuple2) => {

        val (columnName, column) = tuple2
        if (df.columns.contains(columnName.toLowerCase)) {

          log.warn(s"Column '$columnName' is already defined. Thus, it will be overwritten by current column definition")
        }

        df.withColumn(columnName, column)
      })
  }
}

case class WithColumnOptions(override val transformationType: String, override val transformationOrder: Int, columns: List[ColumnOption])
  extends TransformationOptions(transformationType, transformationOrder)

object WithColumnOptions {

  implicit def decodeJson: DecodeJson[WithColumnOptions] = DecodeJson.derive[WithColumnOptions]
}

case class ColumnOption(expression: String, alias: String)

object ColumnOption {

  implicit def decodeJson: DecodeJson[ColumnOption] = DecodeJson.derive[ColumnOption]
}

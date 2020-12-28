package it.luca.spark.sql.utils

import org.apache.spark.sql.DataFrame

class DataFrameExtensions(private val dataFrame: DataFrame) {

  def prettySchema: String = s"\n\n${dataFrame.schema.treeString}"

}

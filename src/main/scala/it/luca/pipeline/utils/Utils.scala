package it.luca.pipeline.utils

import org.apache.spark.sql.DataFrame

object Utils {

  def datasetSchema(dataFrame: DataFrame): String = s"\n\n${dataFrame.schema.treeString}"

}

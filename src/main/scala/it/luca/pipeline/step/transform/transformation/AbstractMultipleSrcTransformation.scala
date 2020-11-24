package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.step.transform.option.JoinTransformationOptions
import org.apache.spark.sql.DataFrame

trait AbstractMultipleSrcTransformation[T <: JoinTransformationOptions] {

  def transform(transformationOptions: T, dataframes: DataFrame*): DataFrame

}

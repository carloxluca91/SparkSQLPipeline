package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.step.transform.option.MultipleSourceTransformationOptions
import org.apache.spark.sql.DataFrame

trait AbstractMultipleSrcTransformation[T <: MultipleSourceTransformationOptions] {

  def transform(transformationOptions: T, dataframes: DataFrame*): DataFrame

}

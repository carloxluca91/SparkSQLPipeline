package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.step.transform.option.TransformationOptions
import org.apache.spark.sql.DataFrame

trait SingleSrcTransformation[T <: TransformationOptions] {

  def transform(transformationOptions: T, dataFrame: DataFrame): DataFrame

}
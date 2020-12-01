package it.luca.pipeline.step.transform.common

import org.apache.spark.sql.DataFrame

trait SingleSrcTransformation[T <: TransformationOptions] {

  def transform(transformationOptions: T, dataFrame: DataFrame): DataFrame

}

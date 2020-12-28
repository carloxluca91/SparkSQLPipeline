package it.luca.pipeline.step.transform.transformation.common

import it.luca.pipeline.step.transform.option.common.SingleSrcTransformationOptions
import org.apache.spark.sql.DataFrame

trait SingleSrcTransformation[T <: SingleSrcTransformationOptions] {

  def transform(transformationOptions: T, dataFrame: DataFrame): DataFrame

}

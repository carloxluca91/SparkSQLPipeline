package it.luca.pipeline.step.transform.transformation.common

import it.luca.pipeline.step.transform.option.common.MultipleSrcTransformationOptions
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

trait MultipleSrcTransformation[T <: MultipleSrcTransformationOptions[_]] {

  def transform(transformationOptions: T, dataframeMap: mutable.Map[String, DataFrame]): DataFrame

}

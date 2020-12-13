package it.luca.pipeline.step.transform.common

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

trait MultipleSrcTransformation[T <: MultipleSrcTransformationOptions] {

  def transform(transformationOptions: T, dataframeMap: mutable.Map[String, DataFrame]): DataFrame

}

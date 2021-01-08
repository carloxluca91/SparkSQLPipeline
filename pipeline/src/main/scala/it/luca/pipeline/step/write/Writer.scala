package it.luca.pipeline.step.write

import org.apache.spark.sql.DataFrame

trait Writer[T <: WriteOptions] {

  def write(dataFrame: DataFrame, writeOptions: T): Unit
}

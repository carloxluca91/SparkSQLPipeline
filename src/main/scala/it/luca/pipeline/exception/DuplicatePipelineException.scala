package it.luca.pipeline.exception

import org.apache.spark.sql.Row

case class DuplicatePipelineException(msg: String) extends Throwable(msg)

object DuplicatePipelineException {

  def apply(pipelineName: String, pipelineFiles: Seq[Row]): DuplicatePipelineException = {

    val files = pipelineFiles.map(_.getAs[String](0)).mkString(", ")
    val msg = s"Same pipeline name ('$pipelineName') is associated to ${pipelineFiles.length} different files ($files)"
    DuplicatePipelineException(msg)
  }
}

package it.luca.pipeline.exception

case class EmptyPipelineException(pipelineName: String)
  extends Throwable(s"Unable to retrieve any step to execute within pipeline '$pipelineName'")

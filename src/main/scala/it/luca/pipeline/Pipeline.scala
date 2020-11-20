package it.luca.pipeline

import argonaut._
import Argonaut._
import it.luca.pipeline.step.common.AbstractStep

case class Pipeline(name: String, description: String, steps: Option[List[AbstractStep]])

object Pipeline {

  implicit def PipelineDecodeJson: DecodeJson[Pipeline] =
    jdecode3L(Pipeline.apply)("name", "description", "steps")
}
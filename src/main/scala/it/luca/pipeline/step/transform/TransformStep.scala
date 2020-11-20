package it.luca.pipeline.step.transform

import it.luca.pipeline.step.common.AbstractStep

case class TransformStep(override val name: String,
                         override val description: String,
                         override val stepType: String,
                         override val dataframeId: String)
  extends AbstractStep(name, description, stepType, dataframeId)

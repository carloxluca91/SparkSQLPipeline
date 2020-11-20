package it.luca.pipeline.step.write

import it.luca.pipeline.step.common.AbstractStep

case class WriteStep(override val name: String,
                     override val description: String,
                     override val stepType: String,
                     override val dataframeId: String)
  extends AbstractStep(name, description, stepType, dataframeId)

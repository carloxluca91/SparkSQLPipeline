package it.luca.pipeline.step.read

import it.luca.pipeline.step.common.AbstractStep

case class ReadStep(override val name: String,
                    override val description: String,
                    override val stepType: String,
                    override val dataframeId: String)
                    //srcOptions: SrcOptions)
  extends AbstractStep(name, description, stepType, dataframeId)

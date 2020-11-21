package it.luca.pipeline

import argonaut.Argonaut._
import it.luca.pipeline.json.JsonField
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.read.ReadStep

class PipelineSpec extends UnitSpec {

  private final val name = "pipelineName"
  private final val description = "pipelineDescription"

  s"A ${Pipeline.getClass.getSimpleName.replace("$", "")} object" should
    s"correctly parse both '${JsonField.Name.label}' and '${JsonField.Description.label}' field(s)" in {

    val pipelineString =
      s"""
        |{
        |   "${JsonField.Name.label}": "$name",
        |   "${JsonField.Description.label}": "$description"
        |}
        |""".stripMargin

    val pipelineOpt: Option[Pipeline] = pipelineString.decodeOption[Pipeline]
    assert(pipelineOpt.nonEmpty)
    pipelineOpt match {
      case None =>
      case Some(x) =>

        assert(x.name equalsIgnoreCase name)
        assert(x.description equalsIgnoreCase description)
        assert(x.pipelineStepsOpt.isEmpty)
    }
  }

  it should s"correctly parse '${JsonField.PipelineSteps.label}' field into a " +
    s"${classOf[List[AbstractStep]].getSimpleName} of ${classOf[AbstractStep].getSimpleName} object(s)" in {

    val pipelineString =
      s"""
         |{
         |  "${JsonField.Name.label}": "$name",
         |  "${JsonField.Description.label}": "$description",
         |  "${JsonField.PipelineSteps.label}": [
         |    {
         |      "${JsonField.Name.label}": "$name",
         |      "${JsonField.Description.label}": "$description",
         |      "${JsonField.DataframeId.label}": "ilBudello",
         |      "${JsonField.StepType.label}": "read"
         |    }
         |  ]
         |}
         |""".stripMargin

    val pipelineOpt: Option[Pipeline] = pipelineString.decodeOption[Pipeline]
    assert(pipelineOpt.nonEmpty)
    pipelineOpt match {
      case None =>
      case Some(x) =>

        assert(x.name equalsIgnoreCase name)
        assert(x.description equalsIgnoreCase description)
        assert(x.pipelineStepsOpt.nonEmpty)
        x.pipelineStepsOpt match {
          case None =>
          case Some(y) =>

            assert(y.size == 1)
            assert(y.head.isInstanceOf[ReadStep])
        }
    }
  }
}

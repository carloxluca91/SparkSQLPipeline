package it.luca.pipeline

import argonaut.Argonaut._
import it.luca.pipeline.json.JsonField
import it.luca.pipeline.step.read.ReadStep

class PipelineSpec extends UnitSpec {

  private final val name = "pipelineName"
  private final val description = "pipelineDescription"

  s"A ${Pipeline.getClass.getSimpleName.replace("$", "")} object" should "be correctly parsed" in {

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
        assert(x.steps.isEmpty)
    }
  }

  it should "correctly parse abstract steps" in {

    val pipelineString =
      s"""
         |{
         |  "${JsonField.Name.label}": "$name",
         |  "${JsonField.Description.label}": "$description",
         |  "steps": [
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
        assert(x.steps.nonEmpty)
        x.steps match {
          case None =>
          case Some(y) =>

            assert(y.size == 1)
            assert(y.head.isInstanceOf[ReadStep])
        }
    }
  }
}

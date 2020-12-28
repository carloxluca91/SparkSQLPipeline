package it.luca.pipeline.option

import scopt.OptionParser

object ScoptParser {

  case class InputConfiguration(pipelineName: String = "", jobPropertiesFile: String = "") {

    private def prettyOption(scoptOption: ScoptOption.Value, value: String): String = {

      s"-${scoptOption.shortOption}, --${scoptOption.longOption} (${scoptOption.description}) = '$value'"
    }

    override def toString: String = {

      s"""
        |
        |       ${prettyOption(ScoptOption.PipelineName, pipelineName)},
        |       ${prettyOption(ScoptOption.PropertiesFile, jobPropertiesFile)}
        |""".stripMargin
    }
  }

  val configParser: OptionParser[InputConfiguration] = new OptionParser[InputConfiguration]("scopt 3.5.0") {

   opt[String](ScoptOption.PipelineName.shortOption, ScoptOption.PipelineName.longOption)
     .text(ScoptOption.PipelineName.description)
     .required()
     .action((x, c) => c.copy(pipelineName = x))

    opt[String](ScoptOption.PropertiesFile.shortOption, ScoptOption.PropertiesFile.longOption)
      .text(ScoptOption.PropertiesFile.description)
      .required()
      .action((x, c) => c.copy(jobPropertiesFile = x))
  }
}

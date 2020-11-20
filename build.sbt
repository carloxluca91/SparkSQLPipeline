val argonautVersion = "6.2.2"
val scalaTestVersion = "3.2.0"
val sparkVersion = "2.2.3"

lazy val pipelineRunnerScala = (project in file("."))
  .settings(

    name := "pipelineRunnerScala",
    version := "0.0.1",
    scalaVersion := "2.11.8",
    scalacOptions ++= Seq(

      "-encoding", "UTF-8",
      "-target:jvm-1.8"
    ),

    libraryDependencies ++= Seq(

      "io.argonaut" %% "argonaut" % argonautVersion,
      "io.argonaut" %% "argonaut-scalaz" % argonautVersion,
      "io.argonaut" %% "argonaut-monocle" % argonautVersion,
      "io.argonaut" %% "argonaut-cats" % argonautVersion,
      "org.scalactic" %% "scalactic" % scalaTestVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    )
  )
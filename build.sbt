val sparkVersion = "2.4.0"
val argonautVersion = "6.2.2"
val scoptVersion = "3.5.0"
val scalaTestVersion = "3.2.0"

ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "it.luca"
ThisBuild / version := "0.0.1"
ThisBuild / scalacOptions ++= Seq(

  "-encoding", "UTF-8",
  "-target:jvm-1.8"
)

ThisBuild / assemblyJarName in assembly := s"${name.value}_${version.value}.jar"
ThisBuild / assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val root = (project in file("."))
  .aggregate(sparkSQL, pipeline)
  .settings(
    name := "sparkSQLPipeline"
  )

lazy val sparkSQL = (project in file("sparkSQL"))
  .settings(

    name := "sparkSQL",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.scalactic" %% "scalactic" % scalaTestVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    )
  )

lazy val pipeline = (project in file("pipeline"))
  .settings(

    name := "pipeline",
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % scoptVersion,
      "io.argonaut" %% "argonaut" % argonautVersion,
      "io.argonaut" %% "argonaut-scalaz" % argonautVersion,
      "io.argonaut" %% "argonaut-monocle" % argonautVersion,
      "io.argonaut" %% "argonaut-cats" % argonautVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.scalactic" %% "scalactic" % scalaTestVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  )).dependsOn(sparkSQL)

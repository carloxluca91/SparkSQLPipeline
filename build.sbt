// Dependencies versions
val commonsConfigurationVersion = "1.6"
val sparkVersion = "2.4.0"
val argonautVersion = "6.2.2"
val scoptVersion = "3.5.0"
val scalaTestVersion = "3.2.0"

// Common settings
lazy val commonSettings = Seq(

  scalaVersion := "2.11.12",
  organization := "it.luca",
  version := "0.2.0",

  // Scala compiler options
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-target:jvm-1.8"
  ),

  onChangedBuildSource in Scope.Global := ReloadOnSourceChanges,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided" exclude("commons-configuration", "commons-configuration"),
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" exclude("commons-configuration", "commons-configuration"),
    "org.scalactic" %% "scalactic" % scalaTestVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),

  // Exclude .properties file from packaging
  (unmanagedResources in Compile) := (unmanagedResources in Compile).value
    .filterNot(_.getName.endsWith(".properties")),

  // Merging strategy
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)

// Aggregate projects in order to run tasks on both of them
lazy val sparkSqlPipeline = (project in file("."))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "commons-configuration" % "commons-configuration" % commonsConfigurationVersion,
      "com.github.scopt" %% "scopt" % scoptVersion,
      "io.argonaut" %% "argonaut" % argonautVersion,
      "io.argonaut" %% "argonaut-scalaz" % argonautVersion,
      "io.argonaut" %% "argonaut-monocle" % argonautVersion,
      "io.argonaut" %% "argonaut-cats" % argonautVersion),

    assemblyJarName in assembly := s"${name.value}_${version.value}.jar")
  .aggregate(sparkSql)
  .dependsOn(sparkSql)

lazy val sparkSql = (project in file("sparkSql"))
  .settings(commonSettings)
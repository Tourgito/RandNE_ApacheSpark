val scala3Version = "2.12.14"

lazy val root = project
  .in(file("."))
  .settings(
    name := "RandNE",
    version := "0.1.0",

    scalaVersion := scala3Version,

    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.5"

  )
  

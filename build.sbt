name := "spark-pipeline"

ThisBuild / organization := "com.sidziuk"
ThisBuild / version := "0.0.1"
ThisBuild / scalaVersion := "2.12.13"

val sparkVersion = "3.2.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

lazy val root = (project in file("."))
  .settings(
    Seq(
      libraryDependencies ++= Seq(
        "ch.qos.logback" % "logback-classic" % "1.4.4",
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-sql" % sparkVersion,
      )
    )
  )
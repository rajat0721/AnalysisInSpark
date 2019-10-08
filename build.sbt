name := "lumiq"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"

/*libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0" % "provided")*/

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % sparkVersion
)
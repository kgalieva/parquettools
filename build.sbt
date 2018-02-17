name := "tools"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  val parquetVersion = "1.9.0"
  Seq(
    "org.apache.spark"     %% "spark-core"              % sparkVer withSources(),
    "org.apache.spark"     %% "spark-sql"               % sparkVer withSources(),
    "org.apache.parquet"   % "parquet-avro"             % parquetVersion,
  )
}

fork in run := true
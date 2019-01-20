name := "Algorithms"

version := "1.3.2"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Confluent" at "http://packages.confluent.io/maven/",
  "pub" at "https://repo1.maven.org/maven2/",
  "Hortonworks Releases" at "http://repo.hortonworks.com/content/repositories/releases/"
)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase
libraryDependencies += "org.apache.hbase" % "hbase" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-common
libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-server
libraryDependencies += "org.apache.hbase" % "hbase-server" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-mapreduce
libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.1.0"
// https://mvnrepository.com/artifact/com.hortonworks.shc/shc-core
libraryDependencies += "com.hortonworks.shc" % "shc-core" % "1.1.0.3.0.1.0-187"


// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0" % "runtime"


libraryDependencies += "org.yaml" % "snakeyaml" % "1.18"

//// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.1"
//// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-core
//libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") =>
    MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}




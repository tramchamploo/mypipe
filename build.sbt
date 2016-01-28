import Dependencies._

lazy val commonSettings = Seq(
  name := "mypipe",
  version := "0.0.4-SNAPSHOT",
  organization := "mypipe",
  scalaVersion := "2.11.7",
  exportJars := true,
  retrieveManaged := true,
  parallelExecution in ThisBuild := false,
  resolvers ++= Seq(Resolver.mavenLocal,
    "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
    "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
    "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Twitter Repository" at "http://maven.twttr.com/")
)

lazy val apiDependencies = Seq(
  akkaActor,
  akkaAgent,
  commonsLang,
  jsqlParser,
  jug,
  logback,
  mysqlAsync,
  mysqlBinlogConnectorJava,
  scalaCompiler,
  scalaReflect,
  scalaTest,
  typesafeConfig,
  curator,
  xmlClient
)

lazy val runnerDependencies = Seq(
  typesafeConfig,
  curator
)

lazy val snapshotterDependencies = Seq(
  logback,
  mysqlAsync,
  scalaTest,
  scopt,
  typesafeConfig
)

lazy val producersDependencies = Seq(
  akkaActor,
  typesafeConfig
)

lazy val avroDependencies = Seq(
  avro,
  guava,
  xinject,
  jerseyServlet,
  jerseyCore,
  jsr305,
  rsApi,
  scalaTest,
  scalaReflect,
  schemaRepoBundle
)

lazy val kafkaDependencies = Seq(
  kafka,
  scalaTest,
  schemaRepoBundle
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(name := "mypipe").
  settings(noPublishSettings: _*).
  aggregate(api, producers, runner, snapshotter, myavro, mykafka)

lazy val runner = (project in file("mypipe-runner")).
  settings(commonSettings: _*).
  settings(
    name := "runner",
    fork in run := false,
    libraryDependencies ++= runnerDependencies).
  settings(Format.settings).
  settings(noPublishSettings: _*) dependsOn(api, producers, myavro, mykafka)

lazy val snapshotter = (project in file("mypipe-snapshotter")).
  settings(commonSettings: _*).
  settings(
    name := "snapshotter",
    fork in run := false,
    libraryDependencies ++= snapshotterDependencies).
  settings(Format.settings).
  settings(noPublishSettings: _*) dependsOn(api % "compile->compile;test->test", producers, myavro, mykafka, runner)

lazy val producers = (project in file("mypipe-producers")).
  settings(commonSettings: _*).
  settings(
    name := "producers",
    fork in run := false,
    libraryDependencies ++= producersDependencies).
  settings(Format.settings).
  settings(noPublishSettings: _*) dependsOn(api)

lazy val api = (project in file("mypipe-api")).
  settings(commonSettings: _*).
  settings(
    name := "api",
    fork in run := false,
    libraryDependencies ++= apiDependencies,
    parallelExecution in Test := false).
  settings(Format.settings).
  settings(publishSettings: _*)

lazy val myavro = (project in file("mypipe-avro")).
  settings(commonSettings: _*).
  settings(
    name := "myavro",
    fork in run := false,
    libraryDependencies ++= avroDependencies,
    parallelExecution in Test := false).
  settings(AvroCompiler.settingsCompile).
  settings(Format.settings).
  settings(publishSettings: _*) dependsOn(api % "compile->compile;test->test")

lazy val mykafka = (project in file("mypipe-kafka")).
  settings(commonSettings: _*).
  settings(
    name := "mykafka",
    fork in run := false,
    libraryDependencies ++= kafkaDependencies,
    parallelExecution in Test := false).
  settings(AvroCompiler.settingsTest).
  settings(Format.settings).
  settings(publishSettings: _*) dependsOn(api % "compile->compile;test->test", myavro)

lazy val noPublishSettings = Seq(
  publish := (),
  publishLocal := (),
  publishArtifact := false
)

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishArtifact in Test := false,
  publishTo := {
    val nexus = "http://nexus.jcndev.com/nexus/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "content/repositories/releases")
  },
  credentials += Credentials("Sonatype Nexus Repository Manager", "nexus.jcndev.com", "ltj", "ltj"),
  pomExtra :=
    <developers>
      <developer>
        <id>guohang.bao</id>
        <name>guohang.bao</name>
        <url>http://github.com/tramchamploo</url>
      </developer>
    </developers>
)
resolvers ++= Seq(
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "jgit-repo" at "http://download.eclipse.org/jgit/maven",
  "sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases"
)

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.2")

addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.2")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.1.9")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.0")


#!/bin/sh

git pull
sbt clean package

CLASSPATH="$PWD/mypipe-runner/target/scala-2.11/runner_2.11-0.0.2-SNAPSHOT.jar:$PWD/mypipe-api/target/scala-2.11/api_2.11-0.0.2-SNAPSHOT.jar:$PWD/mypipe-producers/target/scala-2.11/producers_2.11-0.0.2-SNAPSHOT.jar:$PWD/mypipe-avro/target/scala-2.11/myavro_2.11-0.0.2-SNAPSHOT.jar:$PWD/mypipe-kafka/target/scala-2.11/mykafka_2.11-0.0.2-SNAPSHOT.jar"
for i in `find $PWD/lib_managed -name *.jar -type f`; do
    CLASSPATH=$CLASSPATH:$i;
done
echo "Exporting classpath: $CLASSPATH"
export CLASSPATH

pkill -f "mypipe.runner.PipeRunner"

echo "mypipe stopped!"

java mypipe.runner.PipeRunner

echo "mypipe started!"
#/usr/bin/bash

echo $1
echo $2
echo $3

SPARK_DIR=/home/aua400/installed_stuff/spark-2.3.1/

$SPARK_DIR/bin/spark-submit --master $1 --class indexeddataframe.BenchmarkPrograms --driver-memory 40G --driver-cores 8 --executor-memory 15G --executor-cores 4 target/scala-2.11/indexeddf_2.11-1.0.jar " " /var/scratch/aua400/datagen-edges.csv  " " /var/scratch/aua400/datagen-nodes-smallest.csv $2 $1 --conf spark.executor.extraJavaOptions="-javaagent:/home/aua400/updatableGraphs/IndexedDF/lib/jamm-0.3.4-SNAPSHOT.jar" --driver-java-options "-javaagent:/home/aua400/updatableGraphs/IndexedDF/lib/jamm-0.3.4-SNAPSHOT.jar" --jars "/home/aua400/updatableGraphs/IndexedDF/lib/jamm-0.3.4-SNAPSHOT.jar" > $3

# IndexedDF

<b>1. General Description </b>

This project extends the Spark Dataframe API by adding indexing capabilities. The index implemented is an equality index, but that can theoretically be changed to range indexing. The supported operations are:
<ol>
<li>Index Creation</li>
<li>Point Lookups</li>
<li>Appends</li>
</ol>

<b>2. Building the Project</b>

This project is a standalone sbt project, and can be easily built by executing <em>sbt compile</em>, or <em>sbt package</em> from the command line. Additionally, the test suite can be run by executing <em>sbt test</em>.

<b>3. The API</b>

Assuming we have a Spark data frame <em>df</em>, the API is as follows:
<ol>
<li>indexedDF = df.createIndex(columnNumber: Int); here, the returned object is an <b>Indexed Dataframe</b>.</li>
<li>regularDF = indexedDF.getRows(key: AnyVal); this method returns a regular dataframe containing the rows that are indexed by key <b>key</b>.</li>
<li>newIndexedDF = indexedDF.appendRows(df: Dataframe); this method returns an indexed dataframe where the rows of the <b>df</b> dataframe have been appended to <b>indexedDF</b></li>
</ol>

<b>4. Example Code</b>

```Scala
val sparkSession = SparkSession.builder.
   master("local")
   .appName("indexed df test app")
   // this is the number of partitions the indexed data frame will have, so use this judiciously 
   .config("spark.sql.shuffle.partitions", "4")
   .getOrCreate()
    
// we have to make sure to add the [IndexedOperator] strategies and the [ConvertToIndexedOperators] rules
// otherwise, Spark wouldn't know how to deal with the operators on the indexed dataframes
sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ 
                                            sparkSession.experimental.extraStrategies)
sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++
                                               sparkSession.experimental.extraOptimizations)
    
    
// read a dataframe
val df = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .load("/path/to/data")

// index it on column 0 and cache the indexed result
val indexedDF = df.createIndex(0).cache()

// load another dataframe
val anotherDF = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .load("/path/to/data2")
    
indexedDF.createOrReplaceTempView("indexedtable")
anotherDF.createOrReplaceTempView("nonindexedtable")

// assuming that the indexedDF has column col1 and anotherDF has column col2, we can write the following join:
val result = sparkSession.sql("SELECT * from indexedtable JOIN nonindexedtable ON indexedtable.col1 = nonindexedtable.col2")

result.show()
   
```

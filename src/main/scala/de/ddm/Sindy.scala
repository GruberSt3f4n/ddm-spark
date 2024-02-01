package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.mutable.ListBuffer

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    //import implicit conversion to convert Scala objects to Spark Datasets
    import spark.implicits._

    //read all data using the readData method and store it in a map
    inputs.map(input => readData(input, spark))

      //for each dataset, flatten dataset by row => (Column, Value)
      .map(table => {
        val columns = table.columns
        table.flatMap(row => {
          for (i <- columns.indices) yield {
            (columns(i), row.getString(i))
          }
        })
      })

      //combine all datasets into one
      .reduce(_ union _)

      //group (Column, Value) by Column and just keep Value
      .groupByKey(tuple => tuple._2)

      //create set of the values for each key
      .mapGroups { case (key, iter) => iter.map(_._1).toSet }

      //pair each attribute with all other attributes in set (excluding itself) -> create all tasks
      .flatMap(groups => groups
        .map(currentKey =>
          (currentKey, groups.filter(key => key != currentKey))))

      //group pairs by key
      .groupByKey(row => row._1)

      //for each key produce set of keys that are common to it (set of INDs)
      .mapGroups { case (key, iter) => (key, iter.map(_._2).reduce(_ intersect _))}

      //filter where set of common keys is empty (where no INDs were found)
      .filter(row => row._2.nonEmpty)

      //collect results and sort by key
      .collect()
      .sortBy(ind => ind._1)

      //print the results ()
      .foreach(ind => println(ind._1 + " < " + ind._2.mkString(", ")))
  }
}

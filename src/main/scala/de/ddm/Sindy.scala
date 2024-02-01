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

      //union tables and group by values (value, {all_columns_containing_value})
      .reduce(_ union _)
      .groupByKey(tuple => tuple._2)
      .mapGroups { case (key, iter) => iter.map(_._1).toSet }

      //pair each column with all other columns in set (excluding itself) (one set per value)
      .flatMap(groups => groups
        .map(currentKey =>
          (currentKey, groups.filter(key => key != currentKey))))

      //find INDs
      .groupByKey(row => row._1)
      .mapGroups { case (key, iter) => (key, iter.map(_._2).reduce(_ intersect _))}

      //filter where set of common keys is empty (where no INDs were found)
      .filter(row => row._2.nonEmpty)

      //collect results and sort by Column
      .collect()
      .sortBy(ind => ind._1)

      //print the results ()
      .foreach(ind => println(ind._1 + " < " + ind._2.mkString(", ")))
  }
}

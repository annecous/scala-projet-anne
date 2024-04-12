package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType

import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  override def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession.read.options(options).format(format).load(path)
  }

  override def read(path: String): DataFrame = {
    sparkSession.read.option("sep", ",").option("inferSchema", "true").option("header", "true").format("csv").load(path)
  }

  override def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation'")
  }

  def readCSV(path: String, separator: String = ",", inferSchema: Boolean = true, header: Boolean = true): DataFrame = {
    val csvReader = sparkSession.read
      .option("sep", separator)
      .option("header", header.toString)
      .format("csv")

    if (inferSchema) {
      csvReader.option("inferSchema", "true").load(path)
    } else {
      val schema = if (header) {
        val headerSchema = csvReader.option("inferSchema", "false").load(path).schema
        headerSchema
      } else {
        val firstRow = csvReader.option("inferSchema", "false").option("header", "false").load(path)
        val columnNames = (1 to firstRow.columns.length).map("col" + _)
        StructType(columnNames.map(column => StructField(column, StringType, nullable = true)))
      }
      csvReader.schema(schema).load(path)
    }
  }

  override def readHive(table: String): DataFrame = {
    sparkSession.table(table)
  }

  override def readParquet(path: String): DataFrame = {
    sparkSession.read.format("parquet").load(path)
  }
}

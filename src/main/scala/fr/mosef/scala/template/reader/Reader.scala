package fr.mosef.scala.template.reader

import org.apache.spark.sql.DataFrame

trait Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame

  def read(): DataFrame
  def readHive(table: String): DataFrame

  def read(path: String): DataFrame
  def readCSV(path: String, separator: String = ",", inferSchema: Boolean = true, header: Boolean = true): DataFrame

  def readParquet(path: String): DataFrame





}

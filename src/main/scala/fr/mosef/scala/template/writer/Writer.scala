package fr.mosef.scala.template.writer

import java.util.Properties
import org.apache.spark.sql.DataFrame

class Writer {
  def write(dataFrame: DataFrame, path: String, mode: String = "overwrite"): Unit = {
    val properties = new Properties()
    // Charger le fichier application.properties du classpath
    val inputStream = getClass.getClassLoader.getResourceAsStream("application.properties")
    properties.load(inputStream)

    val format = properties.getProperty("format")
    val separator = properties.getProperty("separator")

    format match {
      case "csv" =>
        dataFrame.write
          .option("sep", separator)
          .format(format)
          .mode(mode)
          .save(path)
      case "hive" =>
        dataFrame.write
          .mode(mode)
          .saveAsTable(path)
      case "parquet" =>
        dataFrame.write
          .format(format)
          .mode(mode)
          .save(path)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported write format: $format")
    }
  }

}

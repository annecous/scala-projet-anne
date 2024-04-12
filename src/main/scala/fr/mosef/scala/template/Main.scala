package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.writer.Writer

object Main extends App with Job {

  val cliArgs = args
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }

  val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      print("No input defined")
      sys.exit(1)
    }
  }
  println(SRC_PATH)
  val DST_PATH: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer"
    }
  }

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .enableHiveSupport()
    .getOrCreate()

  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new fr.mosef.scala.template.processor.impl.ProcessorImpl()
  val src_path = SRC_PATH
  val dst_path = DST_PATH

  val inputDF = if (src_path.endsWith(".parquet")) {
    reader.readParquet(src_path)
  } else if (src_path.endsWith(".csv")) {
    reader.readCSV(src_path)
  } else if (src_path.endsWith(".hive")) {
    reader.readHive(src_path)
  } else {
    println("Unsupported file format")
    sys.exit(1)
  }

  val processedDF: DataFrame = processor.process(inputDF, "report2") // Spécification du type de rapport à générer
  // Configuration du Writer

  val writer: Writer = new Writer()

  writer.write(processedDF, dst_path)

}

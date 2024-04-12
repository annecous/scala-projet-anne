package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame, reportType: String): DataFrame = {
    reportType match {
      case "report1" =>
        inputDF.groupBy("categorie_de_produit").agg(countDistinct("sous_categorie_de_produit").alias("count_sous_categories"))

      case "report2" =>
        inputDF.groupBy("sous_categorie_de_produit").agg(countDistinct("reference_fiche").alias("count_cases"))

      case "report3" =>
        inputDF.groupBy("zone_geographique_de_vente").agg(sum("ndeg_de_version").alias("sum_geographique"))

      case _ => throw new IllegalArgumentException("Unsupported report type")
    }
  }

}
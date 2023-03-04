package com.sidziuk.sparkpipeline

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Runner extends App {

  val spark = SparkSession.builder()
    .master(s"local[${Runtime.getRuntime.availableProcessors}]")
    .appName("open-bean-spark-homework")
    .config("spark.master", "local")
    .getOrCreate()

  val pdp = spark.read.parquet("data_input/pdp")

  val soldPerRetailer = pdp
    .groupBy(col("retailer"))
    .agg(
      count("*").as("product_count")
    )
    .orderBy(col("product_count").desc)

    soldPerRetailer.show(false)

    soldPerRetailer.write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv("data_output/1")

  val productsOrderedByListPricePerRetailer = pdp
    .filter(col("list_price").isNotNull)
    .groupBy(col("retailer"))
    .agg(
      collect_list(
        struct(col("product_code"), col("product_name"), col("list_price")) as "top10_products"
      ) as "top10_products"
    )
    .withColumn("top10_products",
      expr(s"slice(array_sort(top10_products,(l, r) -> case when cast(l.list_price as int) > cast(r.list_price as int) then -1 when cast(l.list_price as int) < cast(r.list_price as int) then 1 else 0 end), 1, 10)")
    )

    productsOrderedByListPricePerRetailer.show(false)

    productsOrderedByListPricePerRetailer
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("retailer")
      .parquet("data_output/2")

  val search = spark.read.parquet("data_input/search")

  val perRetailerPerKeywordSponsoredOrganicRanksProducts = search
    .select(
      col("retailer"),
      col("keyword"),
      col("products").getField("product_name").getItem(0) as "product_name",
      col("products").getField("organic") as "organic",
    )
    .withColumn("rank_organic", when(array_contains(col("organic"), "true") and size(col("organic")) === 1, "rank_organic")
      otherwise null)
    .withColumn("rank_sponsored", when(array_contains(col("organic"), "false") and size(col("organic")) === 1, "rank_sponsored")
      otherwise null)
    .withColumn("rank_total", when(size(col("organic")) === 2, "rank_total")
      otherwise null)
    .drop("organic")
    .orderBy(col("retailer").asc, col("keyword").asc, col("rank_total").asc)

  perRetailerPerKeywordSponsoredOrganicRanksProducts.show(false)

  perRetailerPerKeywordSponsoredOrganicRanksProducts.write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .partitionBy("retailer")
    .csv("data_output/3")

}
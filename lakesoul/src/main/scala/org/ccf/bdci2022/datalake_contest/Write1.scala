package org.ccf.bdci2022.datalake_contest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}

object Write1 {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
        .config("spark.sql.parquet.columnarReaderBatchSize", 256)
      .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
        .config("hive.exec.default.partition.name", "null")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val spark = builder.getOrCreate()

    val dataPath0 = "/home/huazeng/test/parquet/base-0.parquet"
    val dataPath1 = "/home/huazeng/test/parquet/base-1.parquet"
    val dataPath2 = "/home/huazeng/test/parquet/base-2.parquet"
    val dataPath3 = "/home/huazeng/test/parquet/base-3.parquet"
    val dataPath4 = "/home/huazeng/test/parquet/base-4.parquet"
    val dataPath5 = "/opt/spark/work-dir/data/base-5.parquet"
    val dataPath6 = "/opt/spark/work-dir/data/base-6.parquet"
    val dataPath7 = "/opt/spark/work-dir/data/base-7.parquet"
    val dataPath8 = "/opt/spark/work-dir/data/base-8.parquet"
    val dataPath9 = "/opt/spark/work-dir/data/base-9.parquet"
    val dataPath10 = "/opt/spark/work-dir/data/base-10.parquet"


    val tablePath = "/home/huazeng/test/table/table_test1"
    val df = spark.read.format("parquet").option("header", true).load(dataPath0).toDF()

    val df2 = df
//        .where(expr("gender = \"Female\"" ))
        .where(expr("gender = \"Female\" or gender = \"Male\"" ))
//    df2.show()
    df2.write.format("lakesoul").mode("Overwrite")
        .option("rangePartitions","gender")
        .save(tablePath)

    overWriteTable(spark, tablePath, dataPath1)
    println("overWriteTable1 Done")
    overWriteTable(spark, tablePath, dataPath2)
    println("overWriteTable2 Done")
    overWriteTable(spark, tablePath, dataPath3)
    println("overWriteTable3 Done")
    overWriteTable(spark, tablePath, dataPath4)
    println("overWriteTable4 Done")
  }

  def overWriteTable(spark: SparkSession, tablePath: String, path: String): Unit = {
    val df1 = spark.read.format("lakesoul").load(tablePath)
    df1.show(20)
    val df2 = spark.read.format("parquet").load(path).where(expr("gender = \"Female\"" ))
    df2.show(20)
    val joined_df = df1.join(df2, Seq("id"),"full").select(
      col("id"),
      when(df2("ip_address").isNotNull, df2("ip_address")).otherwise(df1("ip_address")).alias("ip_address"),
      when(df2("first_name").isNotNull && df2("first_name").notEqual("null"), df2("first_name")).otherwise(df1("first_name")).alias("first_name"),
      when(df2("last_name").isNotNull && df2("last_name").notEqual("null"), df2("last_name")).otherwise(df1("last_name")).alias("last_name"),
      when(df2("cc").isNotNull, df2("cc")).otherwise(df1("cc")).alias("cc"),
      when(df2("country").isNotNull, df2("country")).otherwise(df1("country")).alias("country"),
      when(df2("birthdate").isNotNull, df2("birthdate")).otherwise(df1("birthdate")).alias("birthdate"),
      when(df2("salary").isNotNull, df2("salary")).otherwise(df1("salary")).alias("salary"),
      when(df2("comments").isNotNull, df2("comments")).otherwise(df1("comments")).alias("comments"),
      when(df2("title").isNotNull, df2("title")).otherwise(df1("title")).alias("title"),
      when(df2("email").isNotNull, df2("email")).otherwise(df1("email")).alias("email"),
      when(df2("gender").isNotNull, df2("gender")).otherwise(df1("gender")).alias("gender"),
    )
    joined_df.show()
    joined_df
        .repartition(5)
        .write.mode("Overwrite").format("lakesoul")
        .option("rangePartitions","gender")
        .save(tablePath)
  }

}

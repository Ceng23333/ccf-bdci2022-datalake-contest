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
        .config("spark.sql.parquet.columnarReaderBatchSize", 128)
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
    val dataPath3 = "/opt/spark/work-dir/data/base-3.parquet"
    val dataPath4 = "/opt/spark/work-dir/data/base-4.parquet"
    val dataPath5 = "/opt/spark/work-dir/data/base-5.parquet"
    val dataPath6 = "/opt/spark/work-dir/data/base-6.parquet"
    val dataPath7 = "/opt/spark/work-dir/data/base-7.parquet"
    val dataPath8 = "/opt/spark/work-dir/data/base-8.parquet"
    val dataPath9 = "/opt/spark/work-dir/data/base-9.parquet"
    val dataPath10 = "/opt/spark/work-dir/data/base-10.parquet"


    val tablePath = "/home/huazeng/test/table/table_test1"
    val df = spark.read.format("parquet").option("header", true).load(dataPath0).toDF()

//    val df2 = df.where(expr("gender = \"Female\" or gender = \"Male\"" ))
    val df2 = df
        .where(expr("gender = \"Female\"" ))
//    df2.show()
    df2.write.format("lakesoul").mode("Overwrite")
        .option("rangePartitions","gender")
        .save(tablePath)

    overWriteTable(spark, tablePath, dataPath1)
    overWriteTable(spark, tablePath, dataPath2)
  }

  def overWriteTable(spark: SparkSession, tablePath: String, path: String): Unit = {
    val df1 = spark.read.format("lakesoul").load(tablePath)
    df1.show(20)
    val df2 = spark.read.format("parquet").load(path).where(expr("gender = \"Female\"" ))
    df2.show(20)
    df1.join(df2, Seq("id"),"full").select(
      col("id"),
      when(df2("ip_address").isNotNull, df2("ip_address")).otherwise(df1("ip_address")).alias("ip_address"),
      when(df2("first_name").isNotNull && df2("first_name").notEqual("null"), df2("first_name")).otherwise(df1("first_name")).alias("first_name"),
      when(df2("country").isNotNull, df2("country")).otherwise(df1("country")).alias("country"),
      when(df2("email").isNotNull, df2("email")).otherwise(df1("email")).alias("email"),
      when(df2("gender").isNotNull, df2("gender")).otherwise(df1("gender")).alias("gender"),
    ).write.mode("Overwrite").format("lakesoul")
        .option("rangePartitions","gender").save(tablePath)
  }

}

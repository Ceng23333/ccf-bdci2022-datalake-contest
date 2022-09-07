package org.ccf.bdci2022.datalake_contest

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.ccf.bdci2022.datalake_contest.Write1.overWriteTable

object Database {
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
            builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9090")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

        val spark = builder.getOrCreate()
//        spark.sparkContext.setLogLevel("DEBUG")

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

        spark.sql("SHOW NAMESPACES")
        spark.sql("SHOW CURRENT NAMESPACE").show()
//        spark.sql("CREATE NAMESPACE test")
        LakeSoulCatalog.createNamespace(Array("test"))
//        spark.sql("USE NAMESPACE test")
        LakeSoulCatalog.useNamespace(Array("test"))
//        spark.sql("SHOW CURRENT NAMESPACE")
        println("showCurrentNamespace=" + LakeSoulCatalog.showCurrentNamespace().head)

        val tablePath = "/home/huazeng/test/table/table_test0"
        val df0 = spark.read.format("parquet").option("header", true).load(dataPath0).toDF()

        df0.where(expr("gender = \"Female\" or gender = \"Male\"" ))
            .write.format("lakesoul")
            .mode("Overwrite")
            .option("rangePartitions","gender")
            .save(tablePath)


//        spark.sql("USE NAMESPACE default")
        LakeSoulCatalog.useNamespace(Array("default"))
        //        spark.sql("SHOW CURRENT NAMESPACE")
        println("showCurrentNamespace=" + LakeSoulCatalog.showCurrentNamespace().head)

        val tablePath1 = "/home/huazeng/test/table/table_test1"
        val df1 = spark.read.format("parquet").option("header", true).load(dataPath1).toDF()

        df1.where(expr("gender = \"Female\" or gender = \"Male\"" ))
            .write.format("lakesoul")
            .mode("Overwrite")
            .option("rangePartitions","gender")
            .save(tablePath)
//        spark.sql("SHOW TABLES FROM default")
        println(LakeSoulCatalog.listTables(Array("default")))
        println(LakeSoulCatalog.listTables(Array("test")))
//        spark.sql("SHOW TABLES FROM test")

    }
}

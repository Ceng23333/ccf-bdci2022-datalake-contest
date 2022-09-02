ls /home/huazeng/.m2/repository/com/dmetasoul
lakesoul_spark="/home/huazeng/\\.m2/repository/com/dmetasoul/lakesoul-spark/2.1.0-lakesoul-spark-3.1.2-SNAPSHOT/lakesoul-spark-2.1.0-lakesoul-spark-3.1.2-SNAPSHOT.jar"
ls $lakesoul_spark
lakesoul_common="/home/huazeng/\\.m2/repository/com/dmetasoul/lakesoul-common/2.1.0-SNAPSHOT/lakesoul-common-2.1.0-SNAPSHOT.jar"
ls $lakesoul_common
cp $lakesoul_spark  /opt/spark/jars
cp $lakesoul_common /opt/spark/jars
spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem  --conf hadoop.fs.s3a.committer.name=directory --conf spark.hadoop.fs.s3a.committer.staging.conflict-mode=append --conf spark.hadoop.fs.s3a.committer.staging.tmp.path=/opt/spark/work-dir/s3a_staging --conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3.buffer.dir=/opt/spark/work-dir/s3 --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.fast.upload=true --conf spark.sql.parquet.columnarReaderBatchSize=256 --conf spark.hadoop.fs.s3a.multipart.size=67108864 --conf spark.sql.parquet.mergeSchema=false --conf spark.sql.parquet.filterPushdown=true --conf spark.hadoop.mapred.output.committer.class=org.apache.hadoop.mapred.FileOutputCommitter --conf spark.sql.warehouse.dir=s3://ccf-datalake-contest/datalake_table/ --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf hive.exec.default.partition.name=null --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider

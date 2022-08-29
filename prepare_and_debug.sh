ls /home/huazeng/.m2/repository/com/dmetasoul
lakesoul_spark="/home/huazeng/\\.m2/repository/com/dmetasoul/lakesoul-spark/2.1.0-lakesoul-spark-3.1.2-SNAPSHOT/lakesoul-spark-2.1.0-lakesoul-spark-3.1.2-SNAPSHOT.jar"
ls $lakesoul_spark
lakesoul_common="/home/huazeng/\\.m2/repository/com/dmetasoul/lakesoul-common/2.1.0-SNAPSHOT/lakesoul-common-2.1.0-SNAPSHOT.jar"
ls $lakesoul_common
sudo cp $lakesoul_spark  /opt/spark/jars
sudo cp $lakesoul_common /opt/spark/jars
spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
sudo rm -rf /home/huazeng/test/table
docker exec -ti lakesoul-compose-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
docker run --net lakesoul-compose-env_default --rm -t -v /home/huazeng/ccf-bdci2022-datalake-contest-examples/lakesoul/target/jars:/opt/spark/work-dir  -v /home/huazeng/test:/home/huazeng/test --env LakeSoulLib=/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 spark-submit --driver-memory 3g  --driver-java-options "--add-opens=java.base/java.nio=ALL-UNNAMED  -XX:MaxPermSize=512M"  --class org.ccf.bdci2022.datalake_contest.Write1 /opt/spark/work-dir/datalake_contest.jar --localtest

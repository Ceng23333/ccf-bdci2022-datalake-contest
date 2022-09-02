sudo rm -rf /home/huazeng/test/result
docker run --net lakesoul-compose-huazeng-local-env_default --rm -t -v /home/huazeng/ccf-bdci2022-datalake-contest-examples/lakesoul/target/jars/:/opt/spark/work-dir -v /home/huazeng/test:/home/huazeng/test --env RUST_TEST_NOCAPTURE=1 --env LakeSoulLib=/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 spark-submit --driver-memory 3g --driver-java-options "--add-opens=java.base/java.nio=ALL-UNNAMED  -XX:MaxPermSize=512M" --executor-memory 2g --class org.ccf.bdci2022.datalake_contest.Read --master local[1] /opt/spark/work-dir/datalake_contest.jar --localtest
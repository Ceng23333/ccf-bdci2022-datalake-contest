sudo rm -rf /home/huazeng/test/result
docker run --net lakesoul-compose-env_default --rm -it -v /home/huazeng/ccf-bdci2022-datalake-contest-examples/lakesoul/target/jars:/opt/spark/extra_jars  -v /home/huazeng/test:/home/huazeng/test --env lakesoul_home=/opt/spark/extra_jars/lakesoul.properties swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 bash

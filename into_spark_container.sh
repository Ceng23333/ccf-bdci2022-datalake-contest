set -ex

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"

cd "$DIR"

cp -f prepare_and_debug.sh /home/huazeng/ccf-bdci2022-datalake-contest-examples/lakesoul/target/jars

docker run --net lakesoul-compose-env_default --rm -it -v /home/huazeng/ccf-bdci2022-datalake-contest-examples/lakesoul/target/jars:/opt/spark/work-dir  -v /home/huazeng/test:/home/huazeng/test -v /home/huazeng/.m2/repository/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar:/opt/spark/jars/postgresql-42.3.3.jar -v /home/huazeng/.m2/repository/com/dmetasoul/lakesoul-spark/2.1.0-lakesoul-spark-3.1.2-SNAPSHOT/lakesoul-spark-2.1.0-lakesoul-spark-3.1.2-SNAPSHOT.jar:/opt/spark/jars/lakesoul-spark-2.1.0-lakesoul-spark-3.1.2-SNAPSHOT.jar -v /home/huazeng/\\.m2/repository/com/dmetasoul/lakesoul-common/2.1.0-SNAPSHOT/lakesoul-common-2.1.0-SNAPSHOT.jar:/opt/spark/jars/lakesoul-common-2.1.0-SNAPSHOT.jar --env RUST_BACKTRACE=1 --env LakeSoulLib=/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 bash /opt/spark/work-dir/prepare_and_debug.sh
set -ex

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"

cd "$DIR"

cp prepare_and_debug.sh /home/huazeng/ccf-bdci2022-datalake-contest-examples/lakesoul/target/jars

docker run --net lakesoul-compose-env_default --rm -it -v /home/huazeng/ccf-bdci2022-datalake-contest-examples/lakesoul/target/jars:/opt/spark/work-dir  -v /home/huazeng/test:/home/huazeng/test -v ~/.m2/repository/com/dmetasoul/:/home/huazeng/.m2/repository/com/dmetasoul/ --env RUST_BACKTRACE=1 --env LakeSoulLib=/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 /opt/spark/work-dir/prepare_and_debug.sh
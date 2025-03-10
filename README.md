# CCF BDCI 2022 数据湖流批一体性能挑战赛示例代码

## LakeSoul 示例
在 `lakesoul` 目录下提供了使用 LakeSoul 写入数据并读取的示例代码。

### 本地开发并测试运行 LakeSoul
我们提供了 docker-compose 脚本，可以在本地快速搭建 LakeSoul 所需的运行环境，包括：
- PostgreSQL (作为 LakeSoul 元数据服务存储)
- MinIO (用以本地模拟 S3 存储)
- Spark

使用方法：
1. 安装 Docker，请参考 [Install Docker Engine](https://docs.docker.com/engine/install/)
2. 使用 docker compose 启动 PostgreSQL 和 MinIO 服务：
    ```bash
    cd lakesoul/lakesoul-compose-env
    docker compose up -d
    ```
    然后使用 `docker compose ps` 查看启动状态，当 lakesoul-compose-env-lakesoul-meta-db-1 、lakesoul-compose-env-minio-1 的 STATUS 列都显示 running 时说明启动成功。
3. 测试 Spark 读写 LakeSoul 表
   1. 编译打包 LakeSoul 示例代码，执行 `./build_lakesoul.sh`，将执行后压缩文件datalake.tar.gz放到/opt/spark/work-dir下，解压 tar -xvf datalake.tar.gz
   2. 拉取 Spark 镜像
       ```bash 
       docker pull swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2
       ```
   3. 获取源数据，加压到本地/opt/spark/work-dir/data下
       ```bash
       cd /opt/spark/work-dir/data
       wget https://dmetasoul-bucket.ks3-cn-beijing.ksyuncs.com/lakesoul/CCF/table_test/CCFdata.tar.gz
       tar -xvf CCFdata.tar.gz
       ```
   4. 执行 Spark 作业
       ```bash
       # 清理元数据
       docker exec -ti lakesoul-compose-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
       # 清理 S3(Minio) 数据
       docker run --net lakesoul-compose-env_default --rm -t swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 aws --no-sign-request --endpoint-url http://minio:9000 s3 rm --recursive s3://ccf-datalake-contest/datalake_table/ccf_datalake_contest_table
       # 执行 LakeSoul 作业
       docker run --net lakesoul-compose-env_default --rm -t -v /opt/spark/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 spark-submit --class org.ccf.bdci2022.datalake_contest.Write --master local[4] /opt/spark/work-dir/datalake_contest.jar --localtest
       ```
       执行 Spark 作业时，我们在 main 方法里检测是否传了 `--localtest` 参数，如果是本地测试，则增加 minio 相关的配置项。在提交到评测平台执行时，不会添加这个参数，s3 环境由评测平台默认配置好。

### 打包
在代码根目录下执行：
```bash
./build_lakesoul.sh
```
即会在 `lakesoul/target` 目录下生成 `datalake.tar.gz` 文件，可以提交到赛题评测页面查看评测结果。

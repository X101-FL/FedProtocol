from fedprototype import BaseClient
from pyspark.sql import SparkSession
from fedprototype.envs import SparkEnv
import cloudpickle
import pyspark.serializers
pyspark.serializers.cloudpickle = cloudpickle


class ClientA(BaseClient):

    def __init__(self):
        super().__init__('SimplyTest', 'PartA')

    def run(self):
        self.logger.info("send to part B")
        self.comm.send('PartB', 'test_a_to_b', 'BiuBiuBiu')

        message_obj = self.comm.receive('PartB', 'test_b_to_a')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'YouYouYou'


class ClientB(BaseClient):
    def __init__(self):
        super().__init__('SimplyTest', 'PartB')

    def run(self):
        self.logger.info("send to part A")
        self.comm.send('PartA', 'test_b_to_a', 'YouYouYou')

        message_obj = self.comm.receive('PartA', 'test_a_to_b')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'BiuBiuBiu'


def get_client_spark_rdd(args):
    spark = SparkSession.builder \
        .appName(f"SimplyTest:{args.role}") \
        .master("local[2]") \
        .config("spark.executor.cores", 1)\
        .getOrCreate()
    return spark.sparkContext.parallelize(range(args.paralle_n), numSlices=args.paralle_n)


def get_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', type=str, default=ClientA.__name__,
                        choices=[ClientA.__name__, ClientB.__name__])
    parser.add_argument('--paralle_n', type=int, default=5)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    client = eval(f"{args.role}()")
    rdd = get_client_spark_rdd(args)

    SparkEnv() \
        .add_client(role_name='PartA') \
        .add_client(role_name='PartB') \
        .set_job_id(job_id=client.protocol_name) \
        .set_coordinater(host="127.0.0.1", port=3502) \
        .run(client=client, rdd=rdd) \
        .collect()

# cd test/spark
# python simply_send_receive.py ClientA
# python simply_send_receive.py ClientB

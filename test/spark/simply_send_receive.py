from fedprototype import BaseClient

from pyspark import SparkConf, SparkContext
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
    _spark_conf = SparkConf() \
        .set("spark.task.maxFailures", "4") \
        .set("spark.extraListeners", "fedprototype.spark.TaskFailedListener") \
        .set("spark.jars", "/root/Projects/FedPrototype/java/fedprototype_spark/fedprototype_spark.jar") \
        .set("fed.coordinater.url", f"http://127.0.0.1:6609")

    sc = SparkContext(master='local[2]', conf=_spark_conf)
    return sc.parallelize(range(args.paralle_n), numSlices=args.paralle_n)


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
        .run(client=client, rdd=rdd) \
    
    import time
    time.sleep(10000)

# cd test/spark
# python simply_send_receive.py --role ClientA --paralle_n 3
# python simply_send_receive.py --role ClientB --paralle_n 3

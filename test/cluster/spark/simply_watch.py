import cloudpickle
import pyspark.serializers
from pyspark import SparkContext

from fedprotocol import BaseClient

pyspark.serializers.cloudpickle = cloudpickle


class ClientA(BaseClient):

    def __init__(self):
        super().__init__("SimplyWatch", 'PartA')

    def run(self, iterator):
        for sender, message_name, message_obj in self.comm.watch('PartB.', 'test_b_to_a'):
            self.logger.info(f"get a message from {sender}:{message_name} = {message_obj}")
            assert message_obj == f"hello PartA I'm {sender}"


class ClientB(BaseClient):
    def __init__(self, index):
        super().__init__("SimplyWatch", f'PartB.{index}')

    def run(self, iterator):
        self.comm.send('PartA', 'test_b_to_a', f"hello PartA I'm {self.role_name}")


def get_client_spark_rdd(args):
    sc = SparkContext.getOrCreate()
    return sc.parallelize(range(args.paralle_n), numSlices=args.paralle_n)


def get_args():
    import argparse
    parser = argparse.ArgumentParser(prefix_chars='=')
    parser.add_argument('==role', type=str, choices=[ClientA.__name__, ClientB.__name__])
    parser.add_argument('==part_b_index', type=int, default=0)
    parser.add_argument('==paralle_n', type=int, default=2)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    from fedprotocol.envs import SparkEnv

    args = get_args()
    if args.role == ClientA.__name__:
        client = ClientA()
    else:
        client = ClientB(index=args.part_b_index)

    rdd = get_client_spark_rdd(args)

    ans = SparkEnv() \
        .add_client(role_name='PartA') \
        .add_client(role_name='PartB.1') \
        .add_client(role_name='PartB.2') \
        .set_coordinater_url("http://127.0.0.1:6609") \
        .set_job_id(job_id=client.protocol_name) \
        .run(client=client, rdd=rdd)
    print(f"result:{ans}")


"""
cd test/cluster/spark
spark-submit --master local[1] --num-executors 2 --executor-memory 1g --executor-cores 1 --deploy-mode client \
             --conf spark.executorEnv.PYTHONPATH="/root/Projects/FedPrototype" \
             --conf spark.executor.memoryOverhead=600M \
             --conf spark.task.maxFailures=4 \
             simply_watch.py ==role ClientA ==paralle_n 1

spark-submit --master local[1] --num-executors 2 --executor-memory 1g --executor-cores 1 --deploy-mode client \
             --conf spark.executorEnv.PYTHONPATH="/root/Projects/FedPrototype" \
             --conf spark.executor.memoryOverhead=600M \
             --conf spark.task.maxFailures=4 \
             simply_watch.py ==role ClientB ==part_b_index 1 ==paralle_n 1

spark-submit --master local[1] --num-executors 2 --executor-memory 1g --executor-cores 1 --deploy-mode client \
             --conf spark.executorEnv.PYTHONPATH="/root/Projects/FedPrototype" \
             --conf spark.executor.memoryOverhead=600M \
             --conf spark.task.maxFailures=4 \
             simply_watch.py ==role ClientB ==part_b_index 2 ==paralle_n 1
"""

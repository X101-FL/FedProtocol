import cloudpickle
import pyspark.serializers
from pyspark import SparkContext
from fedprototype import BaseClient
from fedprototype.envs import SparkEnv

pyspark.serializers.cloudpickle = cloudpickle


class ClientA(BaseClient):

    def __init__(self):
        super().__init__('SimplyTest', 'PartA')

    def run(self, iterator):
        self.logger.debug(f"get data:{list(iterator)}")
        self.logger.info("send to part B")
        self.comm.send('PartB', 'test_a_to_b', 'BiuBiuBiu')
        message_obj = self.comm.receive('PartB', 'test_b_to_a')
        self.logger.info(f"receive message : {message_obj}")
        
        from pyspark import TaskContext
        att_num = TaskContext.get().attemptNumber()
        if att_num==0:
            raise Exception("exception for testing recovery")

        assert message_obj == 'YouYouYou'


class ClientB(BaseClient):
    def __init__(self):
        super().__init__('SimplyTest', 'PartB')

    def run(self, iterator):
        self.logger.debug(f"get data:{list(iterator)}")
        self.logger.info("send to part A")
        self.comm.send('PartA', 'test_b_to_a', 'YouYouYou')

        message_obj = self.comm.receive('PartA', 'test_a_to_b')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'BiuBiuBiu'


def get_client_spark_rdd(args):
    sc = SparkContext.getOrCreate()
    return sc.parallelize(range(args.paralle_n), numSlices=args.paralle_n)


def get_args():
    import argparse
    parser = argparse.ArgumentParser(prefix_chars='=')
    parser.add_argument('==role', type=str, default=ClientA.__name__,
                        choices=[ClientA.__name__, ClientB.__name__])
    parser.add_argument('==paralle_n', type=int, default=2)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    client = eval(f"{args.role}()")
    rdd = get_client_spark_rdd(args)

    ans = SparkEnv() \
        .add_client(role_name='PartA') \
        .add_client(role_name='PartB') \
        .set_coordinater_url("http://127.0.0.1:6609") \
        .set_job_id(job_id=client.protocol_name) \
        .run(client=client, rdd=rdd)
    print(f"result:{ans}")

"""
cd test/spark
spark-submit --master yarn --num-executors 2 --executor-memory 1g --executor-cores 1 --deploy-mode client \
             --conf spark.executorEnv.PYTHONPATH="/root/Projects/FedPrototype" \
             --conf spark.executor.memoryOverhead=600M \
             --conf spark.task.maxFailures=4 \
             simply_send_receive_recovery.py ==role ClientA ==paralle_n 1

spark-submit --master yarn --num-executors 2 --executor-memory 1g --executor-cores 1 --deploy-mode client \
             --conf spark.executorEnv.PYTHONPATH="/root/Projects/FedPrototype" \
             --conf spark.executor.memoryOverhead=600M \
             --conf spark.task.maxFailures=4 \
             simply_send_receive_recovery.py ==role ClientB ==paralle_n 1
"""

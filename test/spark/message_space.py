import cloudpickle
import pyspark.serializers
from pyspark import SparkConf, SparkContext

from fedprototype import BaseClient

pyspark.serializers.cloudpickle = cloudpickle


class Level2ClientA(BaseClient):

    def __init__(self):
        super().__init__("Level2", '2A')

    def run(self):
        self.comm.send(receiver='2B',
                       message_name='who am i',
                       message_obj=f'{self.track_path}',
                       flush=True)


class Level1ClientA(BaseClient):
    def __init__(self):
        super().__init__("Level1", '1A')
        self.l2_client1 = Level2ClientA().rename_protocol("Level2#1")
        self.l2_client2 = Level2ClientA().rename_protocol("Level2#2")

    def init(self):
        self.set_sub_client(self.l2_client1,
                            role_bind_mapping={"2A": "1A", "2B": "1B"})
        self.set_sub_client(self.l2_client2,
                            role_bind_mapping={"2A": "1A", "2B": "1B"})
        return self

    def run(self, iterator):
        with self.l2_client1.init():
            self.l2_client1.run()
            self.l2_client1.run()
            self.l2_client1.run()
            self.l2_client1.run()

        self.comm.send(receiver='1B',
                       message_name='who am i',
                       message_obj=f'{self.track_path}',
                       flush=True)

        with self.l2_client2.init():
            self.l2_client2.run()


class Level2ClientB(BaseClient):

    def __init__(self):
        super().__init__("Level2", '2B')

    def run(self):
        message = self.comm.receive(sender='2A', message_name='who am i')
        self.logger.info(f"get message : {message}")
        return message

    def close(self) -> None:
        self.comm.clear()


class Level1ClientB(BaseClient):
    def __init__(self):
        super().__init__("Level1", '1B')
        self.l2_client1 = Level2ClientB().rename_protocol("Level2#1")
        self.l2_client2 = Level2ClientB().rename_protocol("Level2#2")

    def init(self):
        self.set_sub_client(self.l2_client1,
                            role_bind_mapping={"2A": "1A", "2B": "1B"})
        self.set_sub_client(self.l2_client2,
                            role_bind_mapping={"2A": "1A", "2B": "1B"})
        return self

    def run(self, iterator):
        with self.l2_client2.init():
            assert self.l2_client2.run() == "Level1.1A/Level2#2.2A"

        with self.l2_client1.init():
            assert self.l2_client1.run() == "Level1.1A/Level2#1.2A"

        message = self.comm.receive(sender='1A', message_name='who am i')
        self.logger.info(f"Level1B get message : {message}")
        assert message == "Level1.1A"


def get_client_spark_rdd(args):
    sc = SparkContext.getOrCreate()
    return sc.parallelize(range(args.paralle_n), numSlices=args.paralle_n)


def get_args():
    import argparse
    parser = argparse.ArgumentParser(prefix_chars='=')
    parser.add_argument('==role', type=str, default=Level1ClientA.__name__,
                        choices=[Level1ClientA.__name__, Level1ClientB.__name__])
    parser.add_argument('==paralle_n', type=int, default=2)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    from fedprototype.envs import SparkEnv
    args = get_args()
    client = eval(f"{args.role}()")
    rdd = get_client_spark_rdd(args)

    ans = SparkEnv() \
        .add_client(role_name='1A') \
        .add_client(role_name='1B') \
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
             message_space.py ==role Level1ClientA ==paralle_n 2

spark-submit --master yarn --num-executors 2 --executor-memory 1g --executor-cores 1 --deploy-mode client \
             --conf spark.executorEnv.PYTHONPATH="/root/Projects/FedPrototype" \
             --conf spark.executor.memoryOverhead=600M \
             --conf spark.task.maxFailures=4 \
             message_space.py ==role Level1ClientB ==paralle_n 2
"""

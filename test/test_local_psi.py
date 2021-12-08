import logging
import random

import pytest

from jobs.sample.local_psi import ActiveClient, PassiveClient
from tools.log import LoggerFactory
from fedprototype.envs.local.local_env import LocalEnv


def psi(active_client, passive_client):
    env = LocalEnv()
    env.add_client(client=active_client).add_client(client=passive_client)
    env.run()


@pytest.mark.parametrize("execution_number", range(5))
def test_local_psi(execution_number):
    LoggerFactory.LEVEL = logging.INFO

    active_data = random.sample(range(1, 1000), random.randint(1, 500))
    passive_data = random.sample(range(1, 1000), random.randint(1, 500))
    expect_res = sorted(list(set(active_data) & set(passive_data)))

    active_client = ActiveClient(status="PSI")
    passive_client = PassiveClient(status="PSI")

    # psi(active_client, passive_client)
    env = LocalEnv()
    env.add_client(client=active_client, words=active_data).add_client(
        client=passive_client, words=passive_data
    )
    env.run()

    active_res = active_client.get_intersection_result()
    # print(f"active_res is {active_res}")
    passive_res = passive_client.get_intersection_result()

    assert active_res == expect_res and passive_res == expect_res

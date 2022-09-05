ALL_ENV = ['Local', 'TCP', 'Spark']

class Env:
    """ Get environment by setting name """
    
    @classmethod
    def set(cls, name: str = 'Local'):
        assert name in ALL_ENV, f"The current framework only supports {ALL_ENV}"
        if name == 'Local':
            from .local import LocalEnv
            return LocalEnv()
        elif name == 'TCP':
            from .p2p.tcp import TCPEnv
            return TCPEnv()
        elif name == 'Spark':
            from .cluster.spark import SparkEnv
            return SparkEnv()

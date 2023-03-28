# FedProtocol (Developing)

A protocol-based Federated Learning Framework.

## Requirements

- `python >= 3.8`
- `pycrypto >= 2.6.1`: needed for Private Set Intersection (PSI)
- (Optional) Code Formatter:
    - `black`: better code formatter [Installation](https://pypi.org/project/black/)
    - `isort`: sort imports [Installation](https://github.com/PyCQA/isort)

### TCP
- `fastapi`
- `uvicorn[standard]`
- `requests`

### Spark (Developing)
- `python-multipart`
- `pyspark==2.4.8`
- `hdfs`

## Note

由于重构的原因，目前只有针对于 Local 和 TCP 模式下的测试可以运行，Spark 模式还在开发中。


## Acknowledgments

- [delta-mpc/python-psi](https://github.com/delta-mpc/python-psi)

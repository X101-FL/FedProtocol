import socket


def get_free_ip_port():
    ip = socket.gethostbyname(socket.gethostname())
    _s = socket.socket()
    _s.bind(('', 0))
    _, port = _s.getsockname()
    _s.close()
    return ip, port

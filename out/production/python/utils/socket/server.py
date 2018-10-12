import socket
import threading

import time


def tcplink(sock, addr):
    print('Accept new connection from %s:%s...' % addr)
    sock.send(b'Welcome!')
    while True:
        data = sock.recv(1024)
        time.sleep(1)
        if not data or data.decode('utf-8') == 'exit':
            break
        sock.send(('Hello, %s '% data.decode('utf-8')).encode('utf-8'))
    sock.close()
    print('Connection from %s:%s closed' % addr)


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_host = '127.0.0.1'
port = 9999
server_host_port = (server_host, port)
server.bind(server_host_port)

server.listen(5)
print("Waiting for connection...")

def print_str(a, b):
    print('first var is'.__add__(a))
    print('second var is '.__add__(b))

while True:
    sock, addr = server.accept()
    # print("addr is %s:%s" % addr[0], addr[1])

    #此处开启了2个线程————一个接受来自哪里的连接，另一个打印数据而已
    t = threading.Thread(target=tcplink, args=(sock, addr))
    t1 = threading.Thread(target=print_str, args=("var1", "var2"))
    t.start()
    t1.start()


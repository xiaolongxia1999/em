import socket

socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

host = 'www.sina.com.cn'
port = 80
host_and_port = (host, port)
socket1.connect(host_and_port)

data = b'GET / HTTP/1.1\r\nHost: www.sina.com.cn\r\nConnection: close\r\n\r\n'
socket1.send(data)

buffer = []
while True:
    response = socket1.recv(1024)
    print(type(response))
    if response:
        buffer.append(response)
    else:
        break

data_response = b''.join(buffer)
socket1.close()

print(str(data_response))

header, html = data_response.split(b'\r\n\r\n', 1)
print(header.decode('utf-8'))

with open('sina.html','wb') as f:
    f.write(html)

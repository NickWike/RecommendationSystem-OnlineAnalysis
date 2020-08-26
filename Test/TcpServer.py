import socket
import time
import os

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("localhost", 9999))
s.listen(1)
conn, address = s.accept()
print(conn, address)

with open("/home/zh123/PycharmProjects/RS_HttpServer/logs/RS_Http_Server", "r") as f:
    log_lines = f.readlines()
    for line in log_lines[:25]:
        conn.send(line.encode())
        time.sleep(0.5)

conn.close()
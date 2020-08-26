import socket

s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.connect(('127.0.0.1',9999))
i = 0
while True:
    d = s.recv(1024).decode()
    if d.lower() == "end\n":
        s.close()
        break
    print(i,d)
    i += 1
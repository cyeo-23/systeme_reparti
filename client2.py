import os
import socket
import struct
import traceback
import time

def send_one_message(sock, data):
    length = len(data)
    sock.sendall(struct.pack('!I', length))
    sock.sendall(data)

def recv_one_message(sock):
    lengthbuf = recvall(sock, 4)
    length, = struct.unpack('!I', lengthbuf)
    return recvall(sock, length)

def recvall(sock, count):
    fragments = []
    while count:
        chunk = sock.recv(count)
        if not chunk:
            return None
        fragments.append(chunk)
        count -= len(chunk)
    arr = b''.join(fragments)
    return arr

def list(sock, ip):
    filename = 'machines.txt'
    filesize = os.path.getsize(filename)
    message = 'machine'
    send_one_message(sock, message.encode())
    send_one_message(sock, filename.encode())
    send_one_message(sock, str(filesize).encode())
    with open(filename, 'rb') as f:
        data = f.read(1024)
        while data:
            send_one_message(sock, data)
            data = f.read(1024)
    response = recv_one_message(sock).decode()
    if response == 'machine received':
        print(f'Received response from {ip}: {response}')
    print("end")

def map(socket, split, ip):
    message = "map"
    send_one_message(socket, message.encode())
    send_one_message(socket, split.encode())
    response = recv_one_message(socket).decode()

def shuffle(sock, ip):
    message = "run_shuffle"
    send_one_message(sock, message.encode())
    response = recv_one_message(sock).decode()

def reduce(sock, ip):
    message = "reduce"
    send_one_message(sock, message.encode())
    response = recv_one_message(sock).decode()

def main():
    with open("machines.txt", 'r', encoding='utf-8') as file:
        ips = [line.strip() for line in file]
        socks_dict = {}
        splits = ["/tmp/cyeo-23/splits/S0.txt", "/tmp/cyeo-23/splits/S1.txt", "/tmp/cyeo-23/splits/S2.txt"]
        port = 26541  # replace with the server port number
        print(ips)
        for ip in ips:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((ip, port))
            list(client_socket, ip)
            socks_dict[ip] = client_socket

        for i in range(len(splits)):
            try:
                map(socket=socks_dict[ips[i]], ip=ips[i], split=splits[i])
            except Exception as e:
                print(f'Error: {e}')
                traceback.print_exc()

        print(f'MAP FINISHED ')

        for key, server_socket in socks_dict.items():
            try:
                shuffle(sock=server_socket, ip=key)
            except Exception as e:
                print(f'Error: {e}')
                traceback.print_exc()

        
        print(f'Shuffle FINISHED')

        for key, server_socket in socks_dict.items():
            try:
                reduce(sock=server_socket, ip=key)
            except Exception as e:
                print(f'Error: {e}')
                traceback.print_exc()

        print(f'Reduce FINISHED')

        for key, server_socket in socks_dict.items():
            try:
                # send "bye"
                message = 'bye'
                send_one_message(server_socket, message.encode())
            except Exception as e:
                print(f'Error: {e}')
                traceback.print_exc()

if __name__ == '__main__':
    main()

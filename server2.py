import socket
import struct
import threading
import time
import traceback
from itertools import dropwhile

shuffle_list = []
map_list = []

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

def send_word_to_machine(ip, word):
    port = 26541
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.connect((ip, port))

        message = 'shuffle'
        print("here")
        send_one_message(server, message.encode())
        print(f"{word} send")
        send_one_message(server, word.encode())
        print(recv_one_message(server).decode())

        message = 'Bye'
        send_one_message(server, message.encode())
        server.close()
    except Exception as e:
        print(f'Error : {e}')
        traceback.print_exc()

def get_machine(mot):
    with open("machines.txt", 'r', encoding='utf-8') as file:
        ips = [line.strip() for line in file]
        machine = hash(mot) % len(ips)
        ips = sorted(ips)
        return ips[machine]

def map(split):
    file = split
    with open(file, 'r', encoding='utf-8') as file:
        for line in file:
            words = line.split()
            for word in words:
                word = word.strip('.,!?":;()[]{}')
                word = word.lower()
                map_list.append(word)

def reduce(words):
    reduce_dict = {}
    for word in words:
        if word not in reduce_dict:
            reduce_dict[word] = 0
        reduce_dict[word] += 1
    return reduce_dict

def handle_client(client_socket, address):
    print(f'{socket.gethostname()} New client connected: {address}')
    try:
        while True:
            data = recv_one_message(client_socket)
            if not data:
                break
            message = data.decode().strip().lower()
            print(message)

            if message == "shuffle":
                word = recv_one_message(client_socket).decode().strip()
                shuffle_list.append(word)
                send_one_message(client_socket, f"word {word} received".encode())

            elif message == 'map':
                hostname = socket.gethostname()
                local_ip = socket.gethostbyname(hostname)
                print(f"map start on {local_ip}")
                split = recv_one_message(client_socket).decode().strip()
                print(f'{socket.gethostname()} Received split filename from {address}: {split}')
                map(split=split)
                send_one_message(client_socket, "done".encode())

            elif message == 'run_shuffle':
                hostname = socket.gethostname()
                local_ip = socket.gethostbyname(hostname)
                print(f"shuffle start on {local_ip}")
                print(map_list)
                for word in map_list:
                    machine = get_machine(word)
                    print(machine)
                    send_word_to_machine(machine, word)
                send_one_message(client_socket, "done".encode())

            elif message == 'reduce':
                reduce_data = reduce(shuffle_list)
                print(reduce_data)
                send_one_message(client_socket, "done".encode())

            elif message == 'machine':
                filename = recv_one_message(client_socket).decode().strip()
                print(f'{socket.gethostname()} Received filename from {address}: {filename}')

                filesize = int(recv_one_message(client_socket).decode().strip())
                print(f'{socket.gethostname()} Received filesize from {address}: {filesize}')

                with open(filename, 'wb') as f:
                    remaining_bytes = filesize
                    while remaining_bytes > 0:
                        data = recv_one_message(client_socket)
                        if not data:
                            break
                        f.write(data)
                        remaining_bytes -= len(data)
                print(f'{socket.gethostname()} Received the entire file from {address}: {filesize}')
                response = 'machine received'
                send_one_message(client_socket, response.encode())

            elif message == 'bye':
                break
    except Exception as e:
        print(f'{socket.gethostname()} Error handling client {address}: {e}')
        traceback.print_exc()
    finally:
        client_socket.close()
        print(f'{socket.gethostname()} Client disconnected: {address}')

def start_server(port):
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen()
        print(f'{socket.gethostname()} Server listening on port {port}')
        print(shuffle_list)
        while True:
            client_socket, address = server_socket.accept()
            print(f'{socket.gethostname()} Accepted new connection from {address}')
            client_thread = threading.Thread(target=handle_client, args=(client_socket, address))
            client_thread.start()
    except Exception as e:
        print(f'{socket.gethostname()} Error starting server: {e}')
        traceback.print_exc()
    finally:
        server_socket.close()

if __name__ == '__main__':
    port = 26541  # pick any free port you wish that is not used by other students
    start_server(port)

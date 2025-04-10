import socket

TCP_HOST = 'localhost'
TCP_PORT = 9999

def start_client():
    """
    Connect to the TCP server and receive log lines.
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((TCP_HOST, TCP_PORT))
    print(f"Connected to server at {TCP_HOST}:{TCP_PORT}")

    try:
        while True:
            data = client_socket.recv(1024).decode('utf-8')  # Receive data from the server
            if data:
                print(f"Received data: {data}")
            else:
                break  # No more data, server closed the connection
    except Exception as e:
        print(f"Error receiving data: {e}")
    finally:
        client_socket.close()
        print("Closed connection to server.")

if __name__ == '__main__':
    start_client()

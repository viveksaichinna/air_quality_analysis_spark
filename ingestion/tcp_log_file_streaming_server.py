import os
import shutil
import time
import csv
import socket

# TCP Configuration
TCP_HOST = 'localhost'  # The host to bind the server
TCP_PORT = 9999  # The port on which to listen for incoming connections

# Folder Paths
LOG_FOLDER_PATH = '/workspaces/air_quality_analysis_spark/ingestion/data/pending'  # Path to the folder containing log files
PROCESSED_FOLDER_PATH = os.path.join('/workspaces/air_quality_analysis_spark/ingestion/data/', 'processed')  # Subfolder where processed logs will be moved


def create_tcp_server():
    """
    Creates and returns a TCP server socket.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((TCP_HOST, TCP_PORT))  # Bind to the specified host and port
    server_socket.listen(5)  # Listen for up to 5 incoming connections
    print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}...")
    return server_socket


def ensure_processed_folder():
    """
    Ensures that the 'processed' subfolder exists within the log folder.
    If it doesn't exist, it will be created automatically.
    """
    os.makedirs(PROCESSED_FOLDER_PATH, exist_ok=True)  # Create 'processed' folder if not already present


def send_line_to_client(client_socket, line):
    """
    Sends a single line (CSV record) to the connected TCP client.
    
    Arguments:
    client_socket -- The TCP client socket.
    line -- The line to send.
    """
    client_socket.send((line + "\n").encode('utf-8'))  # Send the line to the client as a byte string
    print(f"Sent: {line}")  # Log the sent message


def process_csv_line(line):
    """
    Parses a single CSV line into a list of values.
    
    Arguments:
    line -- The line to be parsed (CSV format).
    
    Returns:
    A list containing the parsed values from the CSV line.
    """
    csv_reader = csv.reader([line])  # Wrap the line as an iterable so it can be processed by csv.reader
    for record in csv_reader:
        return record  # Return the first (and only) record parsed from the line
    return []  # Return an empty list if parsing fails


def process_log_file(file_path, client_socket):
    """
    Processes a log file line by line, sending each line to the connected TCP client.
    After processing, the log file is moved to the 'processed' folder.
    
    Arguments:
    file_path -- The full path to the log file being processed.
    client_socket -- The connected TCP client socket.
    """
    filename = os.path.basename(file_path)  # Extract the filename from the full file path
    print(f"📄 Processing: {filename}")

    try:
        with open(file_path, 'r') as f:  # Open the log file for reading
            for line in f:
                line = line.strip()  # Strip any leading/trailing whitespace
                if line:  # Skip empty lines
                    # Parse the line into a CSV record
                    record = process_csv_line(line)

                    # If a valid record is parsed, send it to the client
                    if record:
                        send_line_to_client(client_socket, str(record))  # Convert record to a string for transmission
                    else:
                        print(f"Skipping invalid line: {line}")  # Log any invalid lines that don't parse correctly

                    time.sleep(0.01)  # Optional delay to avoid overloading the client

    except Exception as e:
        print(f"Error processing {filename}: {e}")  # Log any errors that occur while processing the file
        return

    # Move the processed file to the 'processed' folder
    dest_path = os.path.join(PROCESSED_FOLDER_PATH, filename)  # Destination path for the processed file
    shutil.move(file_path, dest_path)  # Move the file to the 'processed' folder
    print(f"Moved {filename} to 'processed/'")  # Log confirmation that the file has been moved


def accept_client_connections(server_socket):
    """
    Accepts incoming client connections and processes data for each connected client.
    
    Arguments:
    server_socket -- The TCP server socket used to accept client connections.
    """
    try:
        while True:  # Keep the server running indefinitely
            print("Waiting for new client...")
            client_socket, client_address = server_socket.accept()  # Accept a new client connection
            print(f"New client connected: {client_address}")

            # Get all files in LOG_FOLDER_PATH and sort them by filename.
            # Given the naming format (e.g., location-8118-20250125.csv),
            # lexicographical sorting will order by date correctly.
            for filename in sorted(os.listdir(LOG_FOLDER_PATH)):
                file_path = os.path.join(LOG_FOLDER_PATH, filename)  # Full file path

                # Skip directories and ensure it's not the processed folder
                if not os.path.isfile(file_path) or filename == 'processed':
                    continue

                # Process the log file and send its contents to the client
                process_log_file(file_path, client_socket)

            # Close the client socket after processing the files
            client_socket.close()
            print(f"Closed connection with client: {client_address}")

    except Exception as e:
        print(f"Error accepting client connection: {e}")
    finally:
        server_socket.close()


def start_server():
    """
    Main function to start the server, accept client connections, and process log files.
    """
    ensure_processed_folder()  # Ensure the 'processed' folder exists before starting
    server_socket = create_tcp_server()  # Create and start the TCP server
    accept_client_connections(server_socket)  # Accept and process incoming client connections


if __name__ == '__main__':
    start_server()

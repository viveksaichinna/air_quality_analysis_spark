import os
import shutil
import time
import csv
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Kafka broker(s) to connect to
KAFKA_TOPIC = 'your_topic_name'  # Kafka topic to publish messages

# Folder Paths
LOG_FOLDER_PATH = '/path/to/your/logs'  # Path to the folder containing log files
PROCESSED_FOLDER_PATH = os.path.join(LOG_FOLDER_PATH, 'processed')  # Subfolder where processed logs will be moved


def init_kafka_producer():
    """
    Initializes Kafka producer with low-latency settings.
    This will be used to send data to the Kafka topic.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,  # Kafka broker(s)
        linger_ms=10,  # Set to a small value for lower latency (10 ms)
        batch_size=16384  # 16KB batch size for Kafka producer
    )


def ensure_processed_folder():
    """
    Ensures that the 'processed' subfolder exists within the log folder.
    If it doesn't exist, it will be created automatically.
    """
    os.makedirs(PROCESSED_FOLDER_PATH, exist_ok=True)  # Create 'processed' folder if not already present


def send_line_to_kafka(producer, line):
    """
    Sends a single line (CSV record) to Kafka.
    This will flush immediately to ensure real-time delivery.
    
    Arguments:
    producer -- The Kafka producer instance.
    line -- The line to send (it must be encoded as bytes).
    """
    producer.send(KAFKA_TOPIC, value=line.encode('utf-8'))  # Send the line to the Kafka topic
    producer.flush()  # Ensure the message is sent immediately
    print(f"📤 Sent: {line}")  # Print confirmation of sent message


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


def process_log_file(file_path, producer):
    """
    Processes a log file line by line, sending each line to Kafka as a CSV record.
    After processing, the log file is moved to the 'processed' folder.
    
    Arguments:
    file_path -- The full path to the log file being processed.
    producer -- The Kafka producer instance.
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

                    # If a valid record is parsed, send it to Kafka
                    if record:
                        send_line_to_kafka(producer, str(record))  # Convert record to a string for transmission
                    else:
                        print(f"⚠️ Skipping invalid line: {line}")  # Log any invalid lines that don't parse correctly

                    time.sleep(0.01)  # Optional delay to avoid overloading Kafka or the system (can be adjusted)

    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")  # Log any errors that occur while processing the file
        return

    # Move the processed file to the 'processed' folder
    dest_path = os.path.join(PROCESSED_FOLDER_PATH, filename)  # Destination path for the processed file
    shutil.move(file_path, dest_path)  # Move the file to the 'processed' folder
    print(f"✅ Moved {filename} to 'processed/'")  # Log confirmation that the file has been moved


def watch_folder_loop(poll_interval=5):
    """
    Continuously watches the log folder for new files.
    Processes new files as they are added and moves them to the 'processed' folder after processing.
    
    Arguments:
    poll_interval -- Time interval (in seconds) to wait between checks for new files.
    """
    ensure_processed_folder()  # Ensure the 'processed' folder exists before starting
    producer = init_kafka_producer()  # Initialize Kafka producer
    print("👀 Watching for new log files... (Press Ctrl+C to stop)")

    try:
        while True:  # Infinite loop to keep watching for new files
            files_found = False  # Flag to track if any new files were found

            # Iterate through all files in the LOG_FOLDER_PATH
            for filename in os.listdir(LOG_FOLDER_PATH):
                file_path = os.path.join(LOG_FOLDER_PATH, filename)  # Full file path

                # Skip directories and the 'processed' directory itself
                if not os.path.isfile(file_path) or filename == 'processed':
                    continue

                # Process each found log file
                process_log_file(file_path, producer)
                files_found = True  # Flag to indicate that we found and processed a file

            # If no new files were found, log a message and wait for the next poll
            if not files_found:
                print("⏳ No new files. Waiting...")

            time.sleep(poll_interval)  # Wait before checking for new files again

    except KeyboardInterrupt:
        # Graceful exit if the user interrupts the process (Ctrl+C)
        print("\n👋 Exiting on user interrupt (Ctrl+C).")
    finally:
        producer.flush()  # Ensure all pending Kafka messages are sent
        producer.close()  # Close the Kafka producer gracefully
        print("🧹 Kafka producer closed. Goodbye!")  # Log that the Kafka producer was closed


# Start watching the folder when the script is executed
if __name__ == '__main__':
    watch_folder_loop(poll_interval=5)  # Check for new files every 5 seconds
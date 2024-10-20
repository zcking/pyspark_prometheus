import threading
import socket
import time
import subprocess
from pyspark.sql import SparkSession, DataFrame
from pyspark_prometheus.listener import (
    with_prometheus_metrics,
)

import pytest
from unittest.mock import Mock


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="session", autouse=True)
def docker_compose_prometheus():
    """
    Fixture that starts the Prometheus and Pushgateway services using Docker Compose.
    Stops the services after the tests are done.
    """
    # Start the services
    subprocess.run(["docker-compose", "up", "-d"], check=True)

    # Wait for the services to start
    # TODO: Replace with a more robust health check
    time.sleep(3)

    yield  # Test code runs here

    # Stop the services
    subprocess.run(["docker-compose", "down"], check=True)


def start_socket_server(
    host: str, port: int, messages: list[str], delay: float = 1
) -> threading.Thread:
    """
    Starts a dummy socket server that sends a list of messages to connected clients.

    Args:
        host (str): The host address to bind the server to.
        port (int): The port number to bind the server to.
        messages (list): A list of strings to send to clients.
        delay (float): Optional delay (in seconds) between sending messages.
    """

    def server():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"Starting socket server at {host}:{port}")
            s.bind((host, port))
            s.listen(1)
            conn, _ = s.accept()
            with conn:
                print(f"Connected by {conn}")
                for message in messages:
                    conn.sendall(f"{message}\n".encode("utf-8"))
                    time.sleep(delay)

                # time.sleep(2.0)  # Wait for a while before closing the connection
                print("Closing connection")
                conn.close()

    # Start the server in a separate thread
    thread = threading.Thread(target=server)
    thread.daemon = True
    thread.start()
    return thread


@pytest.mark.integration
def test_simple_socket_to_console(spark, mocker):
    spark = with_prometheus_metrics(spark, "http://localhost:9091")

    # Start the dummy server
    start_socket_server(
        host="localhost",
        port=9999,
        messages=['{"name": "Alice"}', '{"name": "Bob"}'],
        delay=0.1,
    )
    df: DataFrame = (
        spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
    )
    assert df.isStreaming

    # Start the streaming query (this will run until the socket closes)
    query = df.writeStream.format("console").queryName("socket_to_console").start()

    # Stop the query
    if not query.awaitTermination(10):
        query.stop()

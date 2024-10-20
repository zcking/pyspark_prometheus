from pyspark.sql import SparkSession
from pyspark_prometheus.listener import (
    with_prometheus_metrics,
    PrometheusStreamingQueryListener,
)

import pytest
from unittest.mock import Mock


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    yield spark
    spark.stop()


def test_on_query_started(spark, mocker):
    mock_pushgateway = mocker.patch("prometheus_client.push_to_gateway")
    listener = PrometheusStreamingQueryListener(
        "http://mock_pushgateway:9091", "test_job"
    )

    event = mocker.Mock()
    event.id = "test_query_id"
    event.name = "test_query_name"

    listener.onQueryStarted(event)

    assert listener._num_active_queries == 1
    assert listener._query_names["test_query_id"] == "test_query_name"
    mock_pushgateway.assert_called()


def test_on_query_progress(spark, mocker):
    mock_pushgateway = mocker.patch("prometheus_client.push_to_gateway")
    listener = PrometheusStreamingQueryListener(
        "http://mock_pushgateway:9091", "test_job"
    )

    event = mocker.Mock()
    event.progress.name = "test_query_name"
    event.progress.durationMs = {"triggerExecution": 1000}
    event.progress.numInputRows = 10
    event.progress.sources = [
        mocker.Mock(description="test_source", processedRowsPerSecond=5)
    ]
    event.progress.sink.numOutputRows = 5

    listener.onQueryProgress(event)

    mock_pushgateway.assert_called()


def test_on_query_terminated(spark, mocker):
    mock_pushgateway = mocker.patch("prometheus_client.push_to_gateway")
    listener = PrometheusStreamingQueryListener(
        "http://mock_pushgateway:9091", "test_job"
    )

    listener._query_names["test_query_id"] = "test_query_name"
    listener._num_active_queries = 1

    event = mocker.Mock()
    event.id = "test_query_id"

    listener.onQueryTerminated(event)

    assert listener._num_active_queries == 0
    mock_pushgateway.assert_called()


def test_on_query_idle(spark, mocker):
    listener = PrometheusStreamingQueryListener(
        "http://mock_pushgateway:9091", "test_job"
    )
    event = mocker.Mock()
    listener.onQueryIdle(event)
    assert listener._num_active_queries == 0


def test_with_prometheus_metrics(spark, mocker):
    num_listeners = len(spark.streams._jsqm.listListeners())

    with_prometheus_metrics(spark, "http://mock_pushgateway:9091")

    assert len(spark.streams._jsqm.listListeners()) == num_listeners + 1

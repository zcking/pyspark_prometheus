from pyspark.sql.streaming import StreamingQueryListener
from prometheus_client import (
    CollectorRegistry,
    Histogram,
    Counter,
    Gauge,
    push_to_gateway,
)

from pyspark.sql import SparkSession
from pyspark.sql.streaming.listener import (
    QueryStartedEvent,
    QueryProgressEvent,
    QueryTerminatedEvent,
)
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PrometheusStreamingQueryListener(StreamingQueryListener):
    """
    A custom Spark StreamingQueryListener that records metrics for Spark Streaming queries
    and pushes them to a Prometheus Pushgateway. The listener records the following metrics:
    - Number of active queries (Gauge)
    - Query duration (Histogram)
    - Input rows (Counter)
    - Processed rows (Counter)
    - Output rows (Counter)
    - Query exceptions (Counter)

    Args:
        push_gateway_url (str): The URL of the Prometheus Pushgateway to push metrics to.
        prom_job_name (str): The name of the Prometheus exported job name to associate with the metrics.
    """

    def __init__(
        self, push_gateway_url: str, prom_job_name: str = "spark_streaming_metrics"
    ):
        """
        Initializes the PrometheusStreamingQueryListener with the given Pushgateway URL and exported job name.

        Args:
            push_gateway_url (str): The URL of the Prometheus Pushgateway to push metrics to.
            prom_job_name (str): The name of the Prometheus exported job name to associate with the metrics.
        """
        self.push_gateway_url = push_gateway_url
        self.job_name = prom_job_name
        self.registry = CollectorRegistry()

        # Get the active spark session's app name
        self.app_name = SparkSession.getActiveSession().sparkContext.appName
        self._num_active_queries = 0
        self._query_names = {}

        # Define Prometheus metrics
        self.active_queries = Gauge(
            "streaming_active_queries",
            "Number of active streaming queries",
            registry=self.registry,
            labelnames=["app_name"],
            namespace="spark",
        )

        # Use a Histogram for query duration
        self.query_duration = Histogram(
            "streaming_query_duration_seconds",
            "Streaming Query Duration",
            registry=self.registry,
            labelnames=["app_name", "query_name"],
            namespace="spark",
        )
        self.query_input_rows = Counter(
            "streaming_query_input_rows",
            "Streaming Query Input Rows",
            registry=self.registry,
            labelnames=["app_name", "query_name"],
            namespace="spark",
        )
        self.query_processed_rows = Counter(
            "streaming_query_processed_rows",
            "Streaming Query Processed Rows",
            registry=self.registry,
            labelnames=["app_name", "query_name", "source"],
            namespace="spark",
        )
        self.num_output_rows = Counter(
            "streaming_query_output_rows",
            "Streaming Query Output Rows",
            registry=self.registry,
            labelnames=["app_name", "query_name"],
            namespace="spark",
        )
        self.query_exceptions = Counter(
            "streaming_query_exceptions",
            "Streaming Query Exceptions",
            registry=self.registry,
            labelnames=["app_name", "query_name"],
            namespace="spark",
        )

    def onQueryStarted(self, event: "QueryStartedEvent") -> None:
        """
        Callback method that is called when a streaming query is started.
        Updates the number of active queries and pushes the metrics to the Prometheus Pushgateway.

        :param event: The QueryStartedEvent object that contains the query information.
        """
        logger.info(f"Query started: {event.id}")
        self._num_active_queries += 1
        logger.info(f"Active queries: {self._num_active_queries}")
        self.active_queries.labels(app_name=self.app_name).set(self._num_active_queries)
        self._query_names[event.id] = event.name if event.name else event.id

        push_to_gateway(
            self.push_gateway_url,
            job=self.job_name,
            registry=self.registry,
            timeout=5.0,
        )

    def onQueryProgress(self, event: "QueryProgressEvent") -> None:
        """
        Callback method that is called when a streaming query makes progress.
        Records the query duration, input rows, processed rows, and output rows metrics.

        :param event: The QueryProgressEvent object that contains the query progress information.
        """
        logger.info(f"Query made progress: {event.progress}")
        query_name = event.progress.name if event.progress.name else "default"

        # Record Prometheus metrics
        self.query_duration.labels(
            app_name=self.app_name, query_name=query_name
        ).observe(event.progress.durationMs["triggerExecution"] / 1000.0)
        self.query_input_rows.labels(app_name=self.app_name, query_name=query_name).inc(
            event.progress.numInputRows
        )

        for source in event.progress.sources:
            self.query_processed_rows.labels(
                app_name=self.app_name, query_name=query_name, source=source.description
            ).inc(source.processedRowsPerSecond)

        self.num_output_rows.labels(app_name=self.app_name, query_name=query_name).inc(
            event.progress.sink.numOutputRows
        )

        # Push metrics to Prometheus Pushgateway
        push_to_gateway(
            self.push_gateway_url,
            job=self.job_name,
            registry=self.registry,
            timeout=5.0,
        )

    def onQueryTerminated(self, event: "QueryTerminatedEvent") -> None:
        """
        Callback method that is called when a streaming query is terminated.
        Updates the number of active queries and pushes the metrics to the Prometheus Pushgateway.

        :param event: The QueryTerminatedEvent object that contains the query information.
        """
        self._num_active_queries -= 1
        logger.info(f"Query terminated: {event.id}")
        logger.info(f"Active queries: {self._num_active_queries}")
        self.active_queries.labels(app_name=self.app_name).set(self._num_active_queries)
        self.query_exceptions.labels(
            app_name=self.app_name, query_name=self._query_names[event.id]
        ).inc()

        push_to_gateway(
            self.push_gateway_url,
            job=self.job_name,
            registry=self.registry,
            timeout=5.0,
        )


def with_prometheus_metrics(spark: SparkSession, push_gateway_url: str) -> SparkSession:
    """
    Adds a PrometheusStreamingQueryListener to the given SparkSession to record metrics for streaming queries
    and push them to the Prometheus Pushgateway.

    :param spark: The SparkSession to add the listener to.
    :param push_gateway_url: The URL of the Prometheus Pushgateway to push metrics to.
    :return: The SparkSession with the PrometheusStreamingQueryListener added.
    """
    listener = PrometheusStreamingQueryListener(push_gateway_url)
    spark.streams.addListener(listener)
    return spark

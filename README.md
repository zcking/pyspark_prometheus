# PySpark Prometheus Integration

This project provides a seamless integration between PySpark and Prometheus for monitoring Spark Structured Streaming applications.

## Features

- Collects metrics from PySpark Streaming Queries
- Exposes metrics in Prometheus format
- Easy integration with existing PySpark applications

## Installation

To install the required dependencies, run:

```bash
pip install -r requirements.txt
```

## Usage

1. Import the necessary modules in your PySpark application:

    ```python
    from pyspark_prometheus import with_prometheus_metrics
    ```

2. Initialize the Prometheus metrics:

    ```python
    spark = SparkSession.builder.master("local").appName("MySparkApp").getOrCreate()
    spark = with_prometheus_metrics(spark, 'http://localhost:9091')
    ```

3. Start your PySpark job as usual. Metrics will be collected and exposed automatically.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue to discuss your ideas.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or support, please open an issue in the repository.
****
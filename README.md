# Spark Streaming RDD Project: Real-Time Fruit Sales Analysis

This project demonstrates a real-time data processing pipeline using **PySpark RDD-based Streaming**. It reads streaming fruit sales data from a source (CSV/JSON), processes it in **Databricks**, calculates metrics, and sends the results to **Kafka**.  

Ingestion → Transformation → Aggregation → Flagging → Final structured output -> Kafka -> Power-BI(Visualization)

---

## **Project Overview**

The pipeline performs the following steps:

1. **Streaming Input**: Reads real-time data files dropped in a monitored folder (`dbfs:/FileStore/Yash/`).  
2. **Tumbling Window**: Groups records into batches of 5 for processing.  
3. **Metrics Calculation**:
                           - Sum and count per fruit.
                           - Average sales per fruit.
                           - Flags fruits with high average sales (threshold = 3.5).  
4. **Output**: Sends processed results as JSON to a Kafka topic (`test-topic`).  
5. **Visualization**: The output can be consumed by Power BI.

---





## License
MIT

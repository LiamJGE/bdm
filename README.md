# Data Management Backbone Project

## Project Overview

This project aims to create a robust **Big Data Management Backbone** for handling large-scale datasets in a structured manner, enabling efficient access for descriptive and predictive analysis. The pipeline is divided into three key zones:

1. **Landing Zone** – Responsible for receiving and organizing raw data from various sources.
2. **Formatted Zone** – Where data is cleaned, reconciled, and formatted for use in further processing.
3. **Exploitation Zone** – The zone where the processed data is stored and analyzed for generating key insights.

## Zones Breakdown

### 1. Landing Zone
The **Landing Zone** is the initial stage of the pipeline, and it has two parts: the **Temporal Landing Zone** and the **Persistent Landing Zone**. 

- **Temporal Landing Zone**: The raw data from multiple sources (e.g., Idealista, OpenBCN) is collected and temporarily stored. The storage format used here is **Avro**, which provides efficient schema handling and compresses data, making it suitable for handling large datasets.
  
- **Persistent Landing Zone**: After the data is cleaned and structured, it is moved to the Persistent Landing Zone for long-term storage. Here, **HBase** is used for storing the data due to its ability to handle large, structured datasets efficiently.

##### Technologies Used
- **File Formats**: Avro for flexible and compressible data storage.
- **Storage**: HDFS (Hadoop Distributed File System) for distributed file storage in the temporal zone, and HBase for persistence.

### 2. Formatted Zone
In the **Formatted Zone**, the raw data from the Landing Zone undergoes a reconciliation process where inconsistent district and neighborhood names are standardized across datasets. The **MongoDB** database was selected to store the cleaned and reconciled data due to its flexibility and ability to handle schema evolution.

##### Technologies Used
- **Storage**: MongoDB for flexible, schema-less storage.
- **Data Processing**: RDD (Resilient Distributed Datasets) were used for their flexibility and to gain experience working with distributed data structures.

### 3. Exploitation Zone
The **Exploitation Zone** is where the data from the formatted zone is analyzed for Key Performance Indicators (KPIs). Two types of KPIs were generated:

1. **Predictive KPI**: A predictive model was developed to forecast the price of rental listings based on attributes such as location, average family income, and neighborhood establishments. A linear regression model was implemented, but with limited accuracy due to the nature of the project.
  
2. **Descriptive KPIs**: Visualizations were created to show correlations between property prices and neighborhood income, as well as relationships between property types and local establishments.

##### Technologies Used
- **Storage**: MongoDB for storing the cleaned datasets, and HDFS for large-scale distributed data storage.
- **Visualization**: Tableau for creating dashboards and visualizing insights.
- **Machine Learning**: A linear regression model for predictive analysis, implemented in Jupyter Notebook.

## Conclusion

The **Big Data Management Backbone** was successfully implemented with a scalable and flexible architecture. The design choices, such as using Avro for efficient data storage and MongoDB for flexible schema handling, were based on the specific needs of handling large datasets with varying structures. This backbone can be used for both real-time and batch analysis in future expansions.
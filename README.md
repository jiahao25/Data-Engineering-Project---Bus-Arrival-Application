# Data-Engineering-Project---Bus-Arrival-Application


# Introduction
In this Data Engineering project, I will showcase on how I design and create a bus arrival application (similar to SG BusLeh). There are 2 goals in this project:
1. To understand and appreciate how stream processing technologies such as Kafka is used in modern applications today
2. To have fun building projects like this

# Tools Used
- Language: Python
- Data Ingestion: Apache Kafka
- Storage: MongoDB
- Orchestration: Apache Airflow
- UI/Visualization: Flask

# Data Used
The data is ingested from the [LTA API](https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html) (see the Bus Arrival section), which updates the bus arrival information in real time! Which means we can technically use the information displayed on the application to see when the bus are coming.

As of Oct 2021, there are about 5000 bus stops in Singapore. However, a limitation in the LTA API of 5000 calls per day means that this project has to be reduced in terms of scope. Therefore, this project will only display bus arrival information belonging to **bus interchanges**.

# Data Architecture/Pipeline
- **Data Ingestion (Kafka)**: A Kafka producer obtains bus interchange data from a CSV file and queries it with the LTA API. Real-time data belonging to these bus interchanges is consumed by a message broker. Related files: ltaProducer.py

- **Storage (MongoDB)**: A Kafka consumer consumes the data and stores it in a key value database MongoDB. Documents are **updated** instead of inserted to reflect the updated bus arrival timings. Related files: mongoConsumer.py

- **Orchestration (Airflow)**: To simulate real time updates of the bus arrival timings, an airflow DAG is scheduled to run both the Kafka producer and consumer in 1 minute intervals. It uses a BashOperator to run both Python files in 1 minute intervals. Related files: kafka_dag.py

- **UI/Visualization (Flask)**: A flask app is created for the user to interact with the program. When the user types the 5 digit code of a bus interchange, it will query the data from the MongoDB database to reflect the bus arrival timings associated with the bus interchange. Related files: Flask folder

- **Other files**: config.ini contains information such as API Key. bus_interchange_code.csv contains 5 digit bus stop code of all bus interchanges in Singapore.


# Spark Application Guide

This guide will walk you through setting up and running the Spark application on your local machine.

## Prerequisites

Before running the application, ensure that the following tools are installed on your system:

- **Apache Spark**: You should have Apache Spark installed. Follow the [official Apache Spark documentation](https://spark.apache.org/docs/latest/) for installation instructions.
- **Python 3.x**: Ensure Python is installed on your system. You can download it from the [official Python website](https://www.python.org/downloads/).
- **Java**: Apache Spark requires Java to run. Ensure you have Java installed. You can download it from the [official Oracle website](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html).

## Steps to Set Up and Run the Application

Clone the repository containing the Spark application to your local machine:
```bash
cd <repository-directory>
git clone <repository-url>
```

Create a virtual environment to isolate the Python dependencies for the project: For Windows:
```bash
python -m venv venv
```

Activate the virtual environment: For Windows:
```bash
.\venv\Scripts\activate
```

Once the virtual environment is activated, install the required Python packages from the requirements.txt file:
```bash
pip install -r requirements.txt
```

Run the Spark application using the following command:
```bash
spark-submit --master local[*] --conf spark.pyspark.python=python file:///D:/BCG/main.py
```
--master local[*]: Runs Spark locally on all available cores ([*] indicates using all available cores).

--conf spark.pyspark.python=python: Ensures that Python is used for the application (important for compatibility with PySpark).

file:///D:/BCG/main.py: Specifies the path to the main.py script on your machine.
Make sure to replace this with the correct path if it's different on your system.
Once the application is running, you should see the Spark job output in the terminal. Verify that the job completes successfully and check the output data (if applicable).

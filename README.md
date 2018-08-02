# Distributed Computing Introduction

Full PySpark Tutorial - Learn the lingo as well as the basics!

All of the files in this repository are Jupyter Notebooks created by me while I was learning myself! I hope that this is of use to some of you! Enjoy :)

## Getting Started - Steps to downloading Spark for Python 3

**Step 1 - Ensure that Java 8 or higher is installed.**  
To check this run the following command in terminal/bash:
```
$ java -version
```

The output should look something like this: 
```
java version "1.8.0_06-ea"
```

If you do not have Java 8 installed, download it [here!](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html#A1097257)

**Step 2 - Download Apache Spark**
Download the latest verison [here!](http://spark.apache.org/downloads.html)

Unzip the contents manually or via the following commandline/bash commands:
```
$ tar -xzf 
```

If you have brew, Spark can also be installed using brew!
Use the following lines:
```
$ brew update
$ brew install scala
$ brew install apache-spark
```

Find where your spark exists, and its version:
```
$ brew info apache-spark
```

Export the SPARK_HOME to that location for example after downloading mine was as follows:
```
/usr/local/Cellar/apache-spark/2.3.1
```

Then before each use run the following lines, or preferabbly add them to your ~/.bashrc:
```
$ export SPARK_HOME='/usr/local/Cellar/apache-spark/2.3.1/libexec'
$ export PYSPARK_DRIVER_PYTHON=jupyter
$ export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
```

Fell free to run `$ pyspark`

## Contents

1. PySpark Part 1 - Defining Spark and its benifits, learn to load in data and how Spark stores it. 
2. PySpark Part 2 - Map Reduce and other helpful functions.
3. PySpark Part 3 - Reducing the amount of data "shuffle" and more lambda functions.
4. Connecting to MongoDb & S3 - 
5. SparkML & SparkSQL

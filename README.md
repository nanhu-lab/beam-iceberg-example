# beam-iceberg-example
An example to show how to make Apache Beam write data to Apache Iceberg,  and read data from Apache Iceberg.
## Requirements
* The [beam-datalake](https://github.com/nanhu-lab/beam-datalake) project needs to be compiled and installed first.  
## Quickstart
Test cases are given in the BeamIcebergTest document.
1. First, create an iceberg table using initIcebergTable(), which contains four fields: id, user_name, user_age, user_remark.
2. Then, use testIcebergWrite() to write the data to Apache Iceberg. In testIcebergWrite(), the simulated data is created, then the simulated data is converted by Apache Beam (converting the user_name to uppercase), and finally written to Apache Iceberg.
3. At last, use testIcebergRead() to read the data out of Apache Iceberg, and then filter according to the user_age, and write the data that meets the criteria to text.  
## Configuration
A few important dependencies are shown below, and others are seen in the pom.xml  
```xml
<properties>  
    <spark.version>3.2.0</spark.version>
    <beam.version>2.41.0</beam.version>
    <iceberg.version>0.14.0</iceberg.version>
</properties>
<dependencies>
    <dependency>
        <groupId>org.nanhulab</groupId>
        <artifactId>beam-datalake</artifactId>
        <version>1.0.0</version>
    </dependency>
    <!-- iceberg -->
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-spark-runtime-3.2_2.12</artifactId>
        <version>${iceberg.version}</version>
    </dependency>
    <!-- beam -->
    <dependency>
        <groupId>org.apache.beam</groupId>
        <artifactId>beam-runners-core-java</artifactId>
        <version>${beam.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.beam</groupId>
        <artifactId>beam-runners-direct-java</artifactId>
        <version>${beam.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.beam</groupId>
        <artifactId>beam-sdks-java-core</artifactId>
        <version>${beam.version}</version>
        <scope>provided</scope>
    </dependency>
    <!-- spark -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.12</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-streaming_2.12</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-hive_2.12</artifactId>
        <version>${spark.version}</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```
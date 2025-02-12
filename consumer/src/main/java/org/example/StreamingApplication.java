package org.example;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class StreamingApplication {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        //System.setProperty("hadoop.home.dir","C:\\hadoop-3.2.2");

        StructType structType = new StructType()
                .add("search", DataTypes.StringType)
                .add("region", DataTypes.StringType)
                .add("current_ts", DataTypes.StringType)
                .add("userId",DataTypes.IntegerType);


        // SparkSession oluştur
        SparkSession spark = SparkSession.builder()
                .appName("Spark MongoDB Atlas Integration")
                .config("spark.mongodb.write.connection.uri", "mongodb+srv://emrhnaxusoft2:u47XgagHHASkKem@e-commerce.tklnf.mongodb.net/?retryWrites=true&w=majority&appName=e-commerce")
                //mongodb+srv://emrhnaxusoft2:u47XgagHHASkKem@e-commerce.tklnf.mongodb.net/?retryWrites=true&w=majority&appName=e-commerce
                .config("spark.mongodb.write.database", "e-commerce")
                .config("spark.mongodb.write.collection", "streamPC")
                .master("local[*]") // Lokalde çalıştırmak için
                .getOrCreate();


        Dataset<Row> loadDataSet = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "e-commerce-stream").load();

        Dataset<Row> rowDataset = loadDataSet.selectExpr("CAST(value AS STRING)");

        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), structType).as("jsontostructs")).select("jsontostructs.*");

        Dataset<Row> pcFilter = valueDS.filter(valueDS.col("search").equalTo("Bilgisayar"));

        //pcFilter.writeStream().format("console").outputMode("append").start().awaitTermination();

        pcFilter.writeStream().trigger(Trigger.ProcessingTime(600000)).foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                rowDataset.write()
                    .format("mongodb")
                    .mode(SaveMode.Append)
                    .save();
            }
        }).start().awaitTermination();

    }
}

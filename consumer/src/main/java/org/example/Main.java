package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

// MongoDB için gerekli Spark paketleri


public class Main {
    public static void main(String[] args) {

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
                .config("spark.mongodb.write.collection", "TimeStamp")
                .master("local[*]") // Lokalde çalıştırmak için
                .getOrCreate();


        Dataset<Row> loadDataSet = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "e-commerce-topic").load();

        Dataset<Row> rowDataset = loadDataSet.selectExpr("CAST(value AS STRING)");

        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), structType).as("jsontostructs")).select("jsontostructs.*");

        // En çok arama yapılan 5 ürün
//        Dataset<Row> searchGroup = valueDS.groupBy("search").count();
//
//        Dataset<Row> searchResult = searchGroup.sort(functions.desc("count")).limit(5);
//
//        System.out.println("1");
//        // MongoDB'ye yaz
//
//        searchResult.write()
//                .format("mongodb")
//                .mode(SaveMode.Overwrite)
//                .save();
//
//        System.out.println("Veriler MongoDB Atlas'a başarıyla yüklendi!");

        // UserID ile kullanıcı hangi ürünleri kaç kere aratmış
//        Dataset<Row> count = valueDS.groupBy("userId", "search").count();
//
//        Dataset<Row> filter = count.filter("count > 7");
//
//        Dataset<Row> pivot = filter.groupBy("userId").pivot("search").count().na().fill(0);
//
//        pivot.write()
//                .format("mongodb")
//                .mode(SaveMode.Overwrite)
//                .save();

        Dataset<Row> current_ts_window = valueDS.groupBy(functions.window(valueDS.col("current_ts"), "30 minute"), valueDS.col("search")).count();

        current_ts_window.write()
                .format("mongodb")
                .mode(SaveMode.Overwrite)
                .save();

        // SparkSession'ı kapat
        spark.stop();
    }

}

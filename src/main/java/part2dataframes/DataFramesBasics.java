package part2dataframes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataFramesBasics {
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFramesBasics")
                .master("local")
                .getOrCreate();
        String filename = "src/main/resources/data/file.csv";
        Dataset<Row> df = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
                .option("header","true")
                .load(filename);
        df.show();
        df.printSchema();

        df.write()

                .format("jdbc")
                .option("url","jdbc:mysql://localhost:3306/schema2")
                .option("dbtable","table")
                .option("user","root")
                .option("driver","com.mysql.jdbc.Driver")
                .option("password","iPhone11Pro")
                .mode(SaveMode.Overwrite)
                .save();
    }
}

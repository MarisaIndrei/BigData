import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVreader {
    public CSVreader(){

    }

    public Dataset<Row> read(String file){
        SparkSession spark =SparkSession
                .builder()
                .appName("Java Spark")
                .config("spark.master", "local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("inferSchema","true")
                .option("header","true")
                .load(file);
        return df;


    }
}

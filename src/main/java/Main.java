import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.avg;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException{
        CSVreader csvREADER = new CSVreader();
        Dataset<Row> df1 = csvREADER.read("C:\\Users\\Marisa\\Downloads\\BigData\\src\\main\\resources\\data\\covid-19.csv");
        Dataset<Row> df2 = csvREADER.read("C:\\Users\\Marisa\\Downloads\\BigData\\src\\main\\resources\\data\\new_covid19.csv");

        df1.show();
        df1.printSchema();

        df2.show();
        df2.printSchema();

        DBwriter dbWRITER = new DBwriter();
        dbWRITER.write(df1,"jdbc:mysql://localhost:3306/schema2","cases1");
        dbWRITER.write(df2,"jdbc:mysql://localhost:3306/schema2","cases2");

       df1.groupBy("School unit name","Gender")
               .avg("Elementary school cases","Middle school cases","High school cases")
               .show(false);
       DBwriter wr = new DBwriter();
       wr.write(df1.groupBy("School unit name","Gender")
       .avg("Elementary school cases","Middle school cases","High school cases")
       ,"jdbc:mysql://localhost:3306/schema2","cases3");
        wr.write(df2.groupBy("School unit name","Gender")
                        .avg("Elementary school cases","Middle school cases","High school cases")
                ,"jdbc:mysql://localhost:3306/schema2","cases4");


        SparkSession spark =SparkSession
                .builder()
                .appName("Java Spark")
                .config("spark.master", "local")
                .getOrCreate();
        Dataset<Row> rdb = spark.read()
                .format("jdbc")
                .option("url","jdbc:mysql://localhost:3306/schema2")
                .option("dbtable","cases1")
                .option("user","root")
                .option("password","iPhone11Pro")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .load();

        rdb.show();
        rdb.printSchema();

        Dataset<Row> df3= rdb.union(df2);
        dbWRITER.write(df3,"jdbc:mysql://localhost:3306/schema2","cases5");
        wr.write(df3.groupBy("School unit name","Gender")
                        .avg("Elementary school cases","Middle school cases","High school cases")
                ,"jdbc:mysql://localhost:3306/schema2","cases6");









    }
}

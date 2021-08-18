import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class DBwriter {
    public DBwriter(){

    }

    public void write(Dataset<Row> df, String url, String table){
        Properties connProperties = new Properties();
        connProperties.put("user","root");
        connProperties.put("password","iPhone11Pro");

        df.write().mode(SaveMode.Overwrite).jdbc(url,table,connProperties);
    }
}

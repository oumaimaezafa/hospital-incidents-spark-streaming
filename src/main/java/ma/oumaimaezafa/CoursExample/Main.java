package ma.oumaimaezafa.CoursExample;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) throws AnalysisException {

        SparkSession sparkSession=SparkSession.builder().appName("Spark SQL ")
                .master("local[*]").getOrCreate();

        Dataset<Row> df1=sparkSession.read().option("header",true).option("inferSchema",true).csv("products.csv");

        df1.printSchema();//les info sur les colonnes
        df1.createTempView("products");
       // sparkSession.sql("select * from products where Price > 200").show();
        sparkSession.sql("select Name , avg(Price) from products group by Name").show();
        //df1.show();

        //df1.where("price>=1000").show();
        //df1.where(col("price").gt(500)).show();
        //le prix moyenne par marque
         //df1.groupBy(col("Name")).avg("Price").show();

        //df1.select(col("Name"),col("Price")).orderBy(col("Price").desc()).show();

    }
}

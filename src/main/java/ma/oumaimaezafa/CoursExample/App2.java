package ma.oumaimaezafa.CoursExample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.sparkproject.jetty.server.session.Session;

import java.util.concurrent.TimeoutException;

public class App2 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession ss=SparkSession.builder()
                //.master("local[*]")
                .appName("Spark streaming")
                .getOrCreate();

        //definir le schema des fichier traites
        StructType schema=new StructType(new StructField[]{
           new StructField("Name", DataTypes.StringType,true, Metadata.empty()),
            new StructField("Price", DataTypes.DoubleType,true, Metadata.empty()),
            new StructField("Quantity", DataTypes.IntegerType,true, Metadata.empty()),

        });

        Dataset<Row> inputTable=ss.readStream().schema(schema).option("header",true).csv("hdfs://namenode:8020/input");
        Dataset<Row>outTable=inputTable.groupBy("Name").count();
        StreamingQuery query= outTable.writeStream().outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime(7000))
                .start();
        query.awaitTermination();
    }
}

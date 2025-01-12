package ma.oumaimaezafa.Tp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class HospitalStreamsQ1 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        // Initialiser la session Spark
        SparkSession sparkSession = SparkSession.builder()
                .appName("Spark Structured Streaming")
                .getOrCreate();

        // Définir le schéma des fichiers incidents.csv
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("title", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
                new StructField("service", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date", DataTypes.TimestampType, true, Metadata.empty())
        });

        // Lire les données en streaming depuis HDFS
        Dataset<Row> inputTables = sparkSession.readStream()
                .schema(schema)
                .option("header", true)
                .csv("hdfs://namenode:8020/input");

        // Supprimer les lignes avec des valeurs NULL pour "service"
        Dataset<Row> cleanedInputTables = inputTables.filter(col("service").isNotNull());

        // Afficher d’une manière continue le nombre d’incidents par service
        Dataset<Row> incidentsByService = cleanedInputTables.groupBy(col("service")).count();

        // Démarrer le streaming et afficher les résultats dans la console
        incidentsByService.writeStream()
                .outputMode("complete") // Mode de sortie pour les agrégations
                .format("console") // Afficher dans la console
                .trigger(Trigger.ProcessingTime(7000)) // Déclencher toutes les 7 secondes
                .start()
                .awaitTermination(); // Attendre la fin du streaming
    }
}

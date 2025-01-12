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

import static org.apache.spark.sql.functions.*;

public class HospitalStreamsQ2 {
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

        // Filtrer les lignes où "date" est NULL
        Dataset<Row> filteredInputTables = inputTables.filter(col("date").isNotNull());

        // Extraire l'année de la colonne "date"
        Dataset<Row> inputTablesWithYear = filteredInputTables.withColumn("year", year(col("date")));

        // Agréger les incidents par année
        Dataset<Row> incidentsByYear = inputTablesWithYear.groupBy("year").count();

        // Trier par nombre d'incidents (décroissant) et sélectionner les deux premières années
        Dataset<Row> topTwoYears = incidentsByYear.orderBy(col("count").desc()).limit(2);

        // Afficher les résultats en continu dans la console
        topTwoYears.writeStream()
                .outputMode("complete") // Mode de sortie pour les agrégations
                .format("console") // Afficher dans la console
                .trigger(Trigger.ProcessingTime(7000)) // Déclencher toutes les 7 secondes
                .start()
                .awaitTermination(); // Attendre la fin du streaming
    }
}

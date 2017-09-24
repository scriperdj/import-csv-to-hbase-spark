package examples.spark;

/**
 * This Spark application imports given CSV files to HBase table
 *
 */
 import org.apache.spark.api.java.*;
 import org.apache.spark.SparkConf;
 import org.apache.spark.api.java.function.Function;
 import org.apache.spark.api.java.function.PairFunction;
 import org.apache.spark.api.java.function.FlatMapFunction;
 import org.apache.spark.broadcast.Broadcast;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.hbase.HBaseConfiguration;
 import org.apache.hadoop.hbase.client.HBaseAdmin;
 import org.apache.hadoop.hbase.client.Mutation;
 import org.apache.hadoop.hbase.client.Put;
 import org.apache.hadoop.hbase.util.Bytes;
 import org.apache.hadoop.hbase.TableName;
 import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 import com.google.gson.JsonObject;
 import com.google.gson.JsonElement;
 import com.google.gson.JsonParser;
 import java.text.SimpleDateFormat;
 import java.text.DateFormat;
 import java.util.TimeZone;
 import java.util.Date;
 import java.io.BufferedReader;
 import java.io.FileReader;
 import java.io.FileNotFoundException;
 import java.io.IOException;
 import java.util.ArrayList;
 import java.util.Arrays;
 import java.util.List;
 import java.util.Iterator;
 import java.util.HashMap;
 import scala.Tuple2;
 import org.apache.spark.sql.SparkSession;
 import org.apache.spark.sql.Dataset;
 import org.apache.spark.sql.Row;
 import java.text.ParseException;
 import java.util.Collections;
 import java.io.File;
 import java.io.FileInputStream;
 import java.io.InputStream;

 import org.yaml.snakeyaml.Yaml;
 import org.yaml.snakeyaml.constructor.Constructor;

public class ImportCsvToHBase
{
    private static final Logger LOG = LoggerFactory.getLogger(ImportCsvToHBase  .class);
    static JsonParser parser = new JsonParser();
    public static void main( String[] args ) throws Exception {
        String appName = "ImportCsvToHBase";
        InputStream input = new FileInputStream(new File(args[0]));
        Yaml yaml = new Yaml(new Constructor(InputParams.class));
        InputParams config = (InputParams) yaml.load(input);

        // Spark Config
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession sparkSession = SparkSession.builder()
              .appName(appName)
              .config(conf)
              .getOrCreate();

        // create connection with HBase
        Configuration configuration = null;
        try{
           configuration = HBaseConfiguration.create();
           configuration.set("hbase.zookeeper.quorum", config.getQuorum());
           configuration.set("hbase.zookeeper.property.clientPort",config.getPort());
           HBaseAdmin.checkHBaseAvailable(configuration);
           LOG.info("------------------HBase is running!------------------");
        } catch (Exception ce){
          ce.printStackTrace();
        }

        // new Hadoop API configuration
    		Job newAPIJobConfiguration = Job.getInstance(configuration);
    		newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, config.getTableName());
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        Broadcast<String> ROW_KEY_B = sc.broadcast(config.getRowKey());
        Broadcast<ArrayList<HashMap<String,String>>> ROW_VALUES_B = sc.broadcast(config.getRowValues());

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = sparkSession.read()
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("file:///" + config.getInputFile())
            .javaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
                @Override
              public Tuple2<ImmutableBytesWritable, Put> call(Row data)
                  throws Exception {

                String[] royKeys =  ROW_KEY_B.value().split(":");
                String key = "";
                for(String k : royKeys){
                  key = key + data.getAs(k) + ":";
                }
                key = key.substring(0, key.length() - 1);
                Put put = new Put(Bytes.toBytes(key));

                for(HashMap<String,String> val : ROW_VALUES_B.value()){
                  String[] cq = val.get("qualifier").toString().split(":");
                  put.add(Bytes.toBytes(cq[0]), Bytes.toBytes(cq[1]),
                      Bytes.toBytes(data.getAs(val.get("value")).toString()));
                }

                return new Tuple2<ImmutableBytesWritable, Put>(
                    new ImmutableBytesWritable(), put);
              }
        }).cache();

        long countr = hbasePuts.count();

        LOG.info("<-----------Total number of rows to be inserted--------->" + countr);

        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());

        sc.stop();


    }
}

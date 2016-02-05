package org.apache.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

public final class DateConversion {
  public static void main(String[] args) throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName("DateConversion").setMaster("local");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    SQLContext sqlContext = new SQLContext(ctx);
   // sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");


    List<String> jsonData = Arrays.asList( "{\"d\":\"2015-02-01\",\"n\":1}");
      JavaRDD<String> dataRDD = ctx.parallelize(jsonData);
      DataFrame data = sqlContext.read().json(dataRDD);
      DataFrame newData = data.select(data.col("d").cast("date"));
      newData.show();
  }
}

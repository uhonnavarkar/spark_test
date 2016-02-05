/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
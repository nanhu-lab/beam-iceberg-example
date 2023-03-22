/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.nanhulab.beam.iceberg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.nanhulab.beam.datalake.DataLakeIO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BeamDataLakeUtil {

  /**
   * create input data, then transform, at last write data to data lake
   * @param format data lake format, such as delta, hudi, iceberg
   * @param path data lake storage path
   * @param sparkConf
   * @param dataLakeOptions
   */
  public static void write2DataLake(String format, String path, SparkConf sparkConf,
                                    Map<String, String> dataLakeOptions, Integer amount){
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    // create input data
    List<User> dataList = generateData(amount);
    PCollection<User> pcMid = pipeline.apply(Create.of(dataList));

    // transform user name to uppercase
    PCollection<Row> pcMid2 = pcMid.apply(ParDo.of(new StrToUpperCaseFn()));

    DataLakeIO.Write write = DataLakeIO.write()
        .withFormat(format)
        .withMode("append")
        .withSchema(getDataLakeSchema())
        .withPath(path)
        .withSparkConf(sparkConf)
        .withOptions(dataLakeOptions);
    if(dataLakeOptions != null){
      write.withOptions(dataLakeOptions);
    }

    // write to data lake
    pcMid2.apply(write);

    // run
    pipeline.run().waitUntilFinish();
  }

  /**
   * read data from data lake, then transform, at last write to text
   * @param format data lake format, such as delta, hudi, iceberg
   * @param path data lake storage path
   * @param sparkConf
   */
  public static void readFromDataLake(String format, String path, SparkConf sparkConf, String outputPath){
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    // read data from data lake
    PCollection<User> pcStart = pipeline.apply(DataLakeIO.<User>read()
        .withFormat(format)
        .withPath(path)
        .withSparkConf(sparkConf)
        .withCoder(SerializableCoder.of(User.class))
        .withRowMapper(getRowMapper())
    );

    // transform, filter out user between the ages of 10 and 90
    PCollection<String> pcMid2 = pcStart.apply(ParDo.of(new FilterByAge()));

    // write data to text
    pcMid2.apply(TextIO.write().to(outputPath));

    // run
    pipeline.run().waitUntilFinish();

  }

  /**
   * Generate simulated data
   * @param amount The amount of data
   * @return
   */
  private static List<User> generateData(int amount){
    List<User> dataList = new ArrayList<>();
    for(int i = 1; i <= amount; i++){
      User data = new User(
          new Random().nextInt(10000000),
          "tom" + i,
          new Random().nextInt(100),
          "The " + i + "th user named Tom"
      );
      dataList.add(data);
    }
    return dataList;
  }

  /**
   * get Data Lake Schema
   * @return
   */
  private static StructType getDataLakeSchema(){
    return new StructType(new StructField[] {
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("user_name", DataTypes.StringType, false, Metadata.empty()),
        new StructField("user_age", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("user_remark", DataTypes.StringType, true, Metadata.empty())
    });
  }

  /**
   * A conversion that converts a username to uppercase
   * DoFn<UserWithSchema, Row>, The first type is the type of input, and the second type is the type of output
   */
  static class StrToUpperCaseFn extends DoFn<User, Row> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      // get an element from pipeline
      User input = context.element();
      // converts a username to uppercase
      String outputStr = input.getUser_name().toUpperCase();

      org.apache.spark.sql.Row row = RowFactory.create(
          input.getId(),
          outputStr,
          input.getUser_age(),
          input.getUser_remark()
      );
      // output
      context.output(row);
    }
  }

  /**
   * filter out user between the ages of 10 and 90
   */
  static class FilterByAge extends DoFn<User, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      // get an element from pipeline
      User input = context.element();

      if(input.getUser_age() >= 10 && input.getUser_age() <= 90){
        System.out.println(input.toString());
        context.output(input.toString());
      }
    }
  }

  /**
   * get RowMapper
   * @return
   */
  private static DataLakeIO.RowMapper<User> getRowMapper(){
    DataLakeIO.RowMapper<User> rowMapper =
        rs -> new User(
            (Integer)rs.getAs("id"),
            (String)rs.getAs("user_name"),
            (Integer)rs.getAs("user_age"),
            (String)rs.getAs("user_remark")
        );
    return rowMapper;
  }
}

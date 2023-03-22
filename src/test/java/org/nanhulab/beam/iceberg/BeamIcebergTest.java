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

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class BeamIcebergTest {
  private String ICEBERG_PATH = "local.db.user";
  private String ICEBERG = "iceberg";

  private String OUTPUT_PATH = "/tmp/beam/output/iceberg/user.txt";

  /**
   * initial Iceberg table. Iceberg table should be created, then beam write data to it.
   */
  @Test
  public void initIcebergTable() {
    SparkSession sparkSession = SparkSession.builder().config(getIcebergSparkConf()).getOrCreate();
    sparkSession.sql("CREATE TABLE " + ICEBERG_PATH + " (" +
        " id INTEGER," +
        " user_name STRING," +
        " user_age INTEGER," +
        " user_remark STRING)" +
        " USING iceberg");
    sparkSession.close();
  }

  /**
   * beam write data to Iceberg
   */
  @Test
  public void testIcebergWrite(){
    BeamDataLakeUtil.write2DataLake( ICEBERG, ICEBERG_PATH, getIcebergSparkConf(), null, 1);
  }

  /**
   * beam read data from Iceberg
   */
  @Test
  public void testIcebergRead(){
    BeamDataLakeUtil.readFromDataLake( ICEBERG, ICEBERG_PATH,  getIcebergSparkConf(),OUTPUT_PATH);
  }

  private SparkConf getIcebergSparkConf(){
    SparkConf sparkConf = new SparkConf().setAppName("iceberg").setMaster("local")
        .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
        .set("spark.sql.catalog.spark_catalog.type","hive")
        .set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.local.type","hadoop")
        .set("spark.sql.catalog.local.warehouse","/tmp/beam/datalake/iceberg");
    return sparkConf;
  }

}

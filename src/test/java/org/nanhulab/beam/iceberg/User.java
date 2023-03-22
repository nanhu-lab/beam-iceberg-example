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

import lombok.Data;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

import java.io.Serializable;

@Data
public class User implements Serializable {
  private Integer id;
  private String user_name;
  private Integer user_age;
  private String user_remark;
  @SchemaCreate
  public User(
      Integer id,
      String user_name,
      Integer user_age,
      String user_remark
  ) {
    this.id = id;
    this.user_name = user_name;
    this.user_age = user_age;
    this.user_remark = user_remark;
  }
}

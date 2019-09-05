/*
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

package com.facebook.presto.spark

import com.facebook.presto.execution.TaskSource
import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

class MapperHolder extends Serializable {
  lazy val mapper = {
    val m = new ObjectMapper with ScalaObjectMapper
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false)
    m.configure(DeserializationFeature.ACCEPT_FLOAT_AS_INT, false)
    m.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    m.disable(MapperFeature.AUTO_DETECT_CREATORS)
    m.disable(MapperFeature.AUTO_DETECT_FIELDS)
    m.disable(MapperFeature.AUTO_DETECT_SETTERS)
    m.disable(MapperFeature.AUTO_DETECT_GETTERS)
    m.disable(MapperFeature.AUTO_DETECT_IS_GETTERS)
    m.disable(MapperFeature.USE_GETTERS_AS_SETTERS)
    m.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
    m.disable(MapperFeature.INFER_PROPERTY_MUTATORS)
    m.disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
//    m.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_CONCRETE_AND_ARRAYS)
    m.enableDefaultTyping(ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE)
    m.registerModule(DefaultScalaModule)
    m
  }
}

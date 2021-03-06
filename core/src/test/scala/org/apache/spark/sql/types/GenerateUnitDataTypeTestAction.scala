/*
 *
 * Copyright 2019 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.types

import org.apache.spark.sql.BaseTestGenerationSpec
import org.apache.spark.sql.test.generator.{Data, Index, Schema}
import org.apache.spark.sql.test.generator.DataType.{getBaseType, getTypeName, DECIMAL, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator.{getDecimal, getLength, isCharOrBinary, isVarString, randomDataGenerator, schemaGenerator}

trait GenerateUnitDataTypeTestAction extends UnitDataTypeTestSpec with BaseTestGenerationSpec {

  override val rowCount = 50

  private def toString(dataTypes: Seq[String]) = {
    assert(dataTypes.size == 1, "Unit data type tests can not manage multiple columns")
    dataTypes.mkString("_")
  }

  override def getTableName(dataTypes: String*): String = s"test_${toString(dataTypes)}"

  override def getTableNameWithDesc(desc: String, dataTypes: String*): String =
    s"test_${desc}_${toString(dataTypes)}"

  override def getIndexName(dataTypes: String*): String = s"idx_${toString(dataTypes)}"

  def genSchema(dataType: ReflectedDataType,
                tableName: String,
                len: String,
                desc: String): Schema = {
    schemaGenerator(
      database,
      tableName,
      r,
      List(
        (dataType, len, desc)
      ),
      List.empty[Index]
    )
  }

  def genData(schema: Schema): Data = randomDataGenerator(schema, rowCount, dataTypeTestDir, r)

  def init(): Unit = {
    for (dataType <- dataTypes) {
      val typeName = getTypeName(dataType)
      val len = getTypeLength(dataType)
      val tableName = getTableName(typeName)
      val schema = genSchema(dataType, tableName, len, "")
      val data = genData(schema)
      data.save()
    }
    for (dataType <- unsignedDataTypes) {
      val typeName = getTypeName(dataType)
      val len = getTypeLength(dataType)
      val tableName = getTableNameWithDesc(extraDesc, typeName)
      val schema = genSchema(dataType, tableName, len, extraDesc)
      val data = genData(schema)
      data.save()
    }
  }

  def loadTestData(typeName: String): Unit

  def loadUnsignedTestData(typeName: String): Unit

  def test(): Unit = {
    init()
    for (dataType <- dataTypes) {
      val typeName = getTypeName(dataType)
      loadTestData(typeName)
    }
    for (dataType <- unsignedDataTypes) {
      val typeName = getTypeName(dataType)
      loadUnsignedTestData(typeName)
    }
  }
}

/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.standalone.internal

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{Operation, OptimisticTransaction}
import io.delta.standalone.actions.{Metadata => MetadataJ}
import io.delta.standalone.exceptions.{ColumnMappingException, ColumnMappingUnsupportedException}
import io.delta.standalone.types.{ArrayType, DataType, FieldMetadata, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import io.delta.standalone.types.FieldMetadata.builder

import io.delta.standalone.internal.DeltaColumnMapping._
import io.delta.standalone.internal.actions.{Metadata, Protocol}
import io.delta.standalone.internal.util.{ConversionUtils, SchemaMergingUtils}
import io.delta.standalone.internal.util.SchemaMergingUtils.{explodeNestedFieldNames, transformColumns}
import io.delta.standalone.internal.util.TestUtils._


class DeltaColumnMappingSuite extends FunSuite {

  // Set the column mapping mode invalid mode and expect unsupported error
  test("unsupported column mapping mode") {
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(simpleSchema, Map.empty),
        newMetadata = metadata(simpleSchema, withMode(Map.empty, "invalid")),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains "The column mapping mode `invalid` is not supported for this " +
        "Delta version. Please upgrade if you want to use this mode.")
  }

  test("unsupported mode change: example: name to none") {
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(simpleSchema, withMode(Map.empty, "name")),
        newMetadata = metadata(simpleSchema, withMode(Map.empty, "none")),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains
        "Changing column mapping mode from name to none is not supported.")
  }

  testWithIdAndNameModes("unsupported protocol version in existing table and no " +
      "protocol upgrade in new metadata") { mode =>
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(simpleSchema, Map.empty),
        newMetadata = metadata(simpleSchema, withMode(Map.empty, mode)),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains "Your current table protocol version does not support " +
        "changing column mapping modes using delta.columnMapping.mode.")
    assert(ex.getMessage contains
        "Required Delta protocol version for column mapping: Protocol(2,5)")
    assert(ex.getMessage contains
        "Your table's current Delta protocol version: Protocol(1,2)")
  }

  testWithIdAndNameModes("unsupported protocol version in existing table and in new metadata") {
    mode => {
      val ex = intercept[ColumnMappingUnsupportedException] {
        verifyAndUpdateMetadataChange(
          oldProtocol = protocol(1, 2),
          oldMetadata = metadata(simpleSchema, Map.empty),
          newMetadata = metadata(
            simpleSchema, withProtocol(withMode(Map.empty, mode), readerV = 1, writerV = 5)),
          isCreatingNewTable = false)
      }
      assert(ex.getMessage contains "Your current table protocol version does not support " +
          "changing column mapping modes using delta.columnMapping.mode.")
      assert(ex.getMessage contains
                 "Required Delta protocol version for column mapping: Protocol(2,5)")
      assert(ex.getMessage contains
                 "Your table's current Delta protocol version: Protocol(1,2)")
    }
  }

  testWithIdAndNameModes("change mode on an existing table") { mode =>
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(simpleSchema, Map.empty),
      newMetadata = metadata(simpleSchema,
                            withProtocol(withMode(Map.empty, mode), readerV = 2, writerV = 5)),
      isCreatingNewTable = false)

    assertMode(updatedMetadata, mode)

    val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedMetadata.schema)
    assert(actIds === simpleSchemaExpIds)
    assert(actPhyNames === simpleSchemaExpPhyNames)
  }

  testWithIdAndNameModes("change mode on an existing table - complex types") { mode =>
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(complexSchema, Map.empty),
      newMetadata = metadata(complexSchema,
                             withProtocol(withMode(Map.empty, mode), readerV = 2, writerV = 5)),
      isCreatingNewTable = false)

    assertMode(updatedMetadata, mode)

    val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedMetadata.schema)
    assert(actIds === complexSchemaExpIds)
    assert(actPhyNames === complexSchemaExpPhyNames)
  }

  testWithIdAndNameModes("change mode on an new table") { mode =>
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(simpleSchema, Map.empty),
      newMetadata = metadata(simpleSchema,
                             withProtocol(withMode(Map.empty, mode), readerV = 2, writerV = 5)),
      isCreatingNewTable = true)

    assertMode(updatedMetadata, mode)

    val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedMetadata.schema)
    assert(actIds === simpleSchemaExpIds)
    actPhyNames.values.foreach(assertUUIDColumnName(_))
  }

  testWithIdAndNameModes("change mode=name on an new table - complex types") { mode =>
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(complexSchema, Map.empty),
      newMetadata = metadata(complexSchema,
                             withProtocol(withMode(Map.empty, mode), readerV = 2, writerV = 5)),
      isCreatingNewTable = true)

    assertMode(updatedMetadata, mode)

    val newSchema = updatedMetadata.schema
    val (actIds, actPhyNames) = extractIdsAndPhyNames(newSchema)
    assert(actIds === complexSchemaExpIds)
    actPhyNames.values.foreach(assertUUIDColumnName(_))
  }

  testWithIdAndNameModes("existing field metadata is preserved") { mode =>
    val schema = struct(
      field("a", new IntegerType)
          .withNewMetadata(builder().putString("test", "testValue").build()),
      field("b", new StringType)
          .withNewMetadata(builder().putString("test2", "testValue2").build()))

    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(schema, Map.empty),
      newMetadata = metadata(schema,
                             withProtocol(withMode(Map.empty, mode), readerV = 2, writerV = 5)),
      isCreatingNewTable = true)

    assertMode(updatedMetadata, mode)

    val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedMetadata.schema)
    assert(actIds === simpleSchemaExpIds)
    actPhyNames.values.foreach(assertUUIDColumnName(_))

    assert(updatedMetadata.schema.get("a").getMetadata.get("test") == "testValue")
    assert(updatedMetadata.schema.get("b").getMetadata.get("test2") == "testValue2")

    // expect three entries (two from column mapping and one existing one)
    assert(updatedMetadata.schema.get("a").getMetadata.getEntries.size() == 3)
    assert(updatedMetadata.schema.get("b").getMetadata.getEntries.size() == 3)
  }

  testWithIdAndNameModes("verify column mapping metadata") { mode =>
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(complexSchema, Map.empty),
      newMetadata = metadata(complexSchema,
                             withProtocol(withMode(Map.empty, mode), readerV = 2, writerV = 5)),
      isCreatingNewTable = false)

    assertMode(updatedMetadata, mode)
    checkColumnIdAndPhysicalNameAssignments(updatedMetadata.schema, NameMapping)
  }

  test("verify column mapping metadata - invalid metadata") {
    {
      // Update one of the field to has duplicate id
      val schemaDuplicateId =
        transformColumns(schemaWithCMMetadata)((_, field, _) => {
          if (field.getName.equals("si")) {
            field.withNewMetadata(
              FieldMetadata.builder().withMetadata(field.getMetadata)
                .putLong(COLUMN_MAPPING_METADATA_ID_KEY, 3L).build())
          } else {
            field
          }
        })

      Seq(IdMapping, NameMapping).foreach(mode => {
        val ex = intercept[ColumnMappingException] {
          checkColumnIdAndPhysicalNameAssignments(schemaDuplicateId, mode)
        }
        assert(ex.getMessage contains
          s"Found duplicated column id `3` in column mapping mode `${mode.name}`")
      })
    }

    {
      // Update one of the fields to have a duplicate physical name
      val schemaDuplicateName =
        transformColumns(schemaWithCMMetadata)((_, field, _) => {
          if (field.getName.equals("si")) {
            field.withNewMetadata(
              FieldMetadata.builder().withMetadata(field.getMetadata)
                .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "sf").build())
          } else {
            field
          }
        })

      Seq(IdMapping, NameMapping).foreach(mode => {
        val ex = intercept[ColumnMappingException] {
          checkColumnIdAndPhysicalNameAssignments(schemaDuplicateName, mode)
        }
        assert(ex.getMessage contains
          s"Found duplicated physical name `s.sf` in column mapping mode `${mode.name}`")
      })
    }
  }

  test("create physical schema") {
    // Reference schema is the schema without any metadata
    val referenceSchema = schemaWithCMMetadata
    val logicalSchema = dropColumnMappingMetadata(schemaWithCMMetadata)

    val physicalSchemaName = createPhysicalSchema(
      logicalSchema, referenceSchema, NameMapping, checkSupportedMode = false)

    // Make sure every field has physical name and no id. Only the physical name is
    // used to read from or write into parquet files
    transformColumns(physicalSchemaName)((_, field, _) => {
      assert(field.getMetadata.contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY))
      assert(!field.getMetadata.contains(COLUMN_MAPPING_METADATA_ID_KEY))
      field
    })

    val physicalSchemaId = createPhysicalSchema(
      logicalSchema, referenceSchema, IdMapping, checkSupportedMode = false)

    // Make sure every field has the parquet field id. This id is used to read from
    // or write data into the parquet files.
    transformColumns(physicalSchemaId)((path, field, _) => {
      assert(field.getMetadata.contains(COLUMN_MAPPING_METADATA_ID_KEY))
      assert(field.getMetadata.contains(PARQUET_FIELD_ID_METADATA_KEY))

      // Make sure the parquet field id is same as the column mapping id
      assert(
        field.getMetadata.get(COLUMN_MAPPING_METADATA_ID_KEY) ===
        field.getMetadata.get(PARQUET_FIELD_ID_METADATA_KEY))

      field
    })
  }

  test("create physical schema - negative cases") {
    val referenceSchema = schemaWithCMMetadata
    val logicalSchema = dropColumnMappingMetadata(schemaWithCMMetadata)

    {
      // Remove the id for one of the columns
      val schemaMissingId = SchemaMergingUtils.transformColumns(referenceSchema)((_, field, _) => {
        if (field.getName.equals("si")) {
          field.withNewMetadata(
            FieldMetadata.builder().withMetadata(field.getMetadata)
                .remove(COLUMN_MAPPING_METADATA_ID_KEY).build())
        } else {
          field
        }
      })

      val ex = intercept[ColumnMappingException] {
        createPhysicalSchema(logicalSchema, schemaMissingId, IdMapping, checkSupportedMode = false)
      }
      assert(ex.getMessage contains
        s"Missing column ID in column mapping mode `id` in the field: si")
    }
    {
      // Remove the physical name for one of the columns
      val schemaMissingId = SchemaMergingUtils.transformColumns(referenceSchema)((_, field, _) => {
        if (field.getName.equals("si")) {
          field.withNewMetadata(
            FieldMetadata.builder().withMetadata(field.getMetadata)
                .remove(COLUMN_MAPPING_PHYSICAL_NAME_KEY).build())
        } else {
          field
        }
      })

      Seq(IdMapping, NameMapping).foreach(mode => {
        val ex = intercept[ColumnMappingException] {
          createPhysicalSchema(logicalSchema, schemaMissingId, mode, checkSupportedMode = false)
        }
        assert(ex.getMessage contains
          s"Missing physical name in column mapping mode `${mode.name}` in the field: si")
      })
    }
  }

  testWithIdAndNameModes("Upgrade to column mapping mode on existing table") { mode =>
    withTempDir { dir =>

      // Create a table and initiate a transaction
      var txn = getTxnWithMaxFeatureSupport(dir.getCanonicalPath)

      // Upgrade the table protocol version to a version that supports column mapping
      // TODO: until a public API is available, change protocol directly on the txn impl
      txn.asInstanceOf[OptimisticTransactionImpl].upgradeProtocolVersion(
        DeltaColumnMapping.MIN_READER_VERSION,
        DeltaColumnMapping.MIN_WRITER_VERSION)

      // Update table with initial schema
      txn.updateMetadata(metadataJ(complexSchema, Map.empty))
      txn.commit(Seq.empty, OP, "test")

      // Upgrade the column mapping mode in the table
      txn = getTxnWithMaxFeatureSupport(dir.getCanonicalPath)
      txn.updateMetadata(txn.metadata().withColumnMappingMode(mode))
      txn.commit(Seq.empty, OP, "test")

      // Verify the columns have column mapping physical name and id assigned
      txn = getTxnWithMaxFeatureSupport(dir.getCanonicalPath)
      assertMode(txn.metadata(), mode)

      val updatedSchema = txn.metadata().getSchema();
      val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedSchema)
      assert(actIds === complexSchemaExpIds)
      assert(actPhyNames === complexSchemaExpPhyNames)
    }
  }

  testWithIdAndNameModes("Create a new table with column mapping mode") { mode =>
    withTempDir { dir =>
      // Create a table and initiate a transaction
      var txn = getTxnWithMaxFeatureSupport(dir.getCanonicalPath)

      // Upgrade the table protocol version to a version that supports column mapping
      // TODO: until a public API is available, change protocol directly on the txn impl
      txn.asInstanceOf[OptimisticTransactionImpl].upgradeProtocolVersion(
        DeltaColumnMapping.MIN_READER_VERSION,
        DeltaColumnMapping.MIN_WRITER_VERSION)

      // Update table with initial schema and column mapping mode
      txn.updateMetadata(metadataJ(complexSchema, withMode(Map.empty, mode)))
      txn.commit(Seq.empty, OP, "test")

      // Verify the columns have column mapping physical name and id assigned
      txn = getTxnWithMaxFeatureSupport(dir.getCanonicalPath)
      assertMode(txn.metadata(), mode)

      val schema = txn.metadata().getSchema();
      val (actIds, actPhyNames) = extractIdsAndPhyNames(schema)
      assert(actIds === complexSchemaExpIds)
      actPhyNames.values.foreach(assertUUIDColumnName(_))
    }
  }

  test("Try to apply an invalid column mapping mode on existing table") {
    withTempDir { dir =>
      // Create a table and initiate a transaction
      val txn = getTxnWithMaxFeatureSupport(dir.getCanonicalPath)

      // Upgrade the table protocol version to a version that supports column mapping
      // TODO: until a public API is available, change protocol directly on the txn impl
      txn.asInstanceOf[OptimisticTransactionImpl].upgradeProtocolVersion(
        DeltaColumnMapping.MIN_READER_VERSION,
        DeltaColumnMapping.MIN_WRITER_VERSION)

      val ex = intercept[ColumnMappingUnsupportedException] {
        // Try to upgrade to an invalid column mapping and expect an error
        txn.updateMetadata(txn.metadata().withColumnMappingMode("invalid"))
      }
      assert(ex.getMessage contains
        "The column mapping mode `invalid` is not supported for this Delta version. " +
          "Please upgrade if you want to use this mode.")
    }
  }

  private def testWithIdAndNameModes(testDescr: String)(testFunc: (String) => Any): Unit = {
    Seq("name", "id").foreach(mode => {
      test(testDescr + s": $mode") {
        testFunc(mode)
      }
    })
  }

  private def getTxnWithMaxFeatureSupport(path: String): OptimisticTransaction = {
    // Create a table with highest possible protocol support (temporary until we have
    // protocol upgrade API)
    val log = getDeltaLogWithMaxFeatureSupport(new Configuration(), path)
    log.startTransaction()
  }

  private def struct(fields: StructField *): StructType = new StructType(fields.toArray)

  private def map(keyType: DataType, valueType: DataType): MapType =
    new MapType(keyType, valueType, true)

  private def array(elementType: DataType): ArrayType = new ArrayType(elementType, true)

  private def field(name: String, dataType: DataType): StructField = new StructField(name, dataType)

  private def metadata(schema: StructType, conf: Map[String, String]): Metadata = {
    Metadata(
      schemaString = schema.toJson,
      configuration = conf
    )
  }

  private def metadataJ(schema: StructType, conf: Map[String, String]): MetadataJ =
    ConversionUtils.convertMetadata(metadata(schema, conf))

  private def protocol(readerVersion: Int, writerVersion: Int): Protocol =
    new Protocol(readerVersion, writerVersion)

  private def withMode(existingConf: Map[String, String], newMode: String): Map[String, String] = {
    val newConf = mutable.Map[String, String]() ++ existingConf
    newConf.put(DeltaConfigs.COLUMN_MAPPING_MODE.key, newMode)
    newConf.toMap
  }

  private def withProtocol(
      existingConf: Map[String, String], readerV: Int, writerV: Int): Map[String, String] = {
    val newConf = mutable.Map[String, String]() ++ existingConf
    newConf.put(Protocol.MIN_READER_VERSION_PROP, readerV.toString)
    newConf.put(Protocol.MIN_WRITER_VERSION_PROP, writerV.toString)
    newConf.toMap
  }

  private def withCMMetdata(field: StructField, id: Int, phyName: String): StructField = {
    field.withNewMetadata(
      FieldMetadata.builder().withMetadata(field.getMetadata)
        .putLong(COLUMN_MAPPING_METADATA_ID_KEY, id)
        .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, phyName)
        .build())
  }

  private def assertUUIDColumnName(physicalName: String): Unit =
    assert(physicalName.toString.startsWith("col-"),
      s"Physical UUID column name doesn't start with col-: $physicalName")

  private def assertMode(metadata: MetadataJ, expectedMode: String): Unit = {
    // Fetch it through the config property and verify
    assert(metadata.getConfiguration.get("delta.columnMapping.mode") == expectedMode)
    // Fetch it through the API on [[Metadata]] and verify
    assert(metadata.getColumnMappingMode == expectedMode)
  }

  private def assertMode(metadata: Metadata, expectedMode: String): Unit = {
    assert(metadata.configuration.get("delta.columnMapping.mode") == Some(expectedMode))
  }

  private def extractIdsAndPhyNames(
      schema: StructType): (Map[Seq[String], Long], Map[Seq[String], String]) = {
    val actIds = mutable.Map[Seq[String], Long]()
    val actPhyNames = mutable.Map[Seq[String], String]()
    transformColumns(schema)((path, field, _) => {
      val colPath = path :+ field.getName
      actIds.put(colPath, field.getMetadata.get(COLUMN_MAPPING_METADATA_ID_KEY).asInstanceOf[Long])
      actPhyNames.put(colPath, field.getMetadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY).toString)
      field
    })
    (actIds.toMap, actPhyNames.toMap)
  }

  private val simpleSchema = struct(field("a", new IntegerType), field("b", new StringType))

  /** Expected column Id for test schema [[simpleSchema]] */
  private val simpleSchemaExpIds = Map(Seq("a") -> 1, Seq("b") -> 2)

  /** Expected physical column names for test schema [[simpleSchema]] */
  private val simpleSchemaExpPhyNames = Map(Seq("a") -> "a", Seq("b") -> "b")

  private val complexSchema = struct(
    field("i", new IntegerType),
    field("str", new StringType),
    field("m", map(new FloatType, new StringType)),
    field("ms", map(
      new FloatType,
      struct(field("msi", new IntegerType), field("msf", new FloatType))
      )
    ),
    field("a", array(new LongType)),
    field("s",
          struct(
            field("sa", new IntegerType),
            field("ss", struct(field("ssstr", new StringType)))
            )
          )
    )

  /** Expected column Id for test schema [[complexSchema]] */
  private val complexSchemaExpIds = Map(
    Seq("i") -> 1,
    Seq("str") -> 2,
    Seq("m") -> 3,
    Seq("ms") -> 4,
    Seq("ms", "value", "msi") -> 5,
    Seq("ms", "value", "msf") -> 6,
    Seq("a") -> 7,
    Seq("s") -> 8,
    Seq("s", "sa") -> 9,
    Seq("s", "ss") -> 10,
    Seq("s", "ss", "ssstr") -> 11
  )

  /** Expected physical column names for test schema [[complexSchema]] */
  private val complexSchemaExpPhyNames = Map(
    Seq("i") -> "i",
    Seq("str") -> "str",
    Seq("m") -> "m",
    Seq("ms") -> "ms",
    Seq("ms", "value", "msi") -> "msi",
    Seq("ms", "value", "msf") -> "msf",
    Seq("a") -> "a",
    Seq("s") -> "s",
    Seq("s", "sa") -> "sa",
    Seq("s", "ss") -> "ss",
    Seq("s", "ss", "ssstr") -> "ssstr"
  )

  private val schemaWithCMMetadata = struct(
    withCMMetdata(field("a", new IntegerType), id = 1, phyName = "a"),
    withCMMetdata(field("b", new StringType), id = 2, phyName = "b"),
    withCMMetdata(
      field("s",
            struct(
              withCMMetdata(field("si", new IntegerType), id = 4, phyName = "si"),
              withCMMetdata(field("sf", new FloatType), id = 5, phyName = "sf"))),
      id = 3,
      phyName = "s")
    )

  private val OP = new Operation(Operation.Name.MANUAL_UPDATE)
}

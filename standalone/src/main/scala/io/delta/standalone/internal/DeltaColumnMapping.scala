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

import java.util.{Locale, UUID}

import scala.collection.mutable

import io.delta.standalone.types.{FieldMetadata, StructField, StructType}

import io.delta.standalone.internal.actions.{Metadata, Protocol}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.exception.DeltaErrors._
import io.delta.standalone.internal.logging.Logging
import io.delta.standalone.internal.util.{CaseInsensitiveMap, SchemaMergingUtils, SchemaUtils}

trait DeltaColumnMappingBase extends Logging {
  // Minimum writer and reader version required for column mapping support
  val MIN_WRITER_VERSION = 5
  val MIN_READER_VERSION = 2
  val MIN_PROTOCOL_VERSION = new Protocol(MIN_READER_VERSION, MIN_WRITER_VERSION)

  // Field id that maps to the field id in the Parquet file. This is used
  // when the column mapping mode is ID
  val PARQUET_FIELD_ID_METADATA_KEY = "parquet.field.id"

  private val COLUMN_MAPPING_METADATA_PREFIX = "delta.columnMapping."

  // Id metadata key for the column - used incase of ID and NAME column mapping modes
  val COLUMN_MAPPING_METADATA_ID_KEY = COLUMN_MAPPING_METADATA_PREFIX + "id"

  // Physical name metadata key for the column - used incase of NAME column mapping mode
  // This could be different from the column name in schema.
  val COLUMN_MAPPING_PHYSICAL_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "physicalName"

  private val supportedModes: Set[DeltaColumnMappingMode] = Set(NoMapping, NameMapping, IdMapping)

  /**
   * This list of internal columns (and only this list) is allowed to have missing
   * column mapping metadata such as field id and physical name because
   * they might not be present in user's table schema.
   *
   * These fields, if materialized to parquet, will always be matched by their display name in the
   * downstream parquet reader even under column mapping modes.
   *
   * For future developers who want to utilize additional internal columns without generating
   * column mapping metadata, please add them here.
   *
   * This list is case-insensitive.
   */
  protected val DELTA_INTERNAL_COLUMNS: Set[String] = Seq(
    "__is_cdc",
    "_change_type",
    "_commit_version",
    "_commit_timestamp").map(_.toLowerCase(Locale.ROOT)).toSet

  /**
   * Update the metadata based on valid column mapping transition. If the column mapping
   * transition is not valid an error is thrown.
   */
  def verifyAndUpdateMetadataChange(
      oldProtocol: Protocol,
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean): Metadata = {

    // field in new metadata should have been dropped
    val oldMappingMode = oldMetadata.columnMappingMode
    val newMappingMode = newMetadata.columnMappingMode

    if (!supportedModes.contains(newMappingMode)) {
      throw unsupportedColumnMappingMode(newMappingMode.name)
    }

    val isChangingModeOnExistingTable = oldMappingMode != newMappingMode && !isCreatingNewTable
    if (isChangingModeOnExistingTable) {
      if (!allowMappingModeChange(oldMappingMode, newMappingMode)) {
        throw changeColumnMappingModeNotSupported(oldMappingMode.name, newMappingMode.name)
      }
      // legal mode change, now check if protocol is upgraded before or part of this update
      val caseInsensitiveMap = CaseInsensitiveMap(newMetadata.configuration)
      val newProtocol = new Protocol(
        minReaderVersion = caseInsensitiveMap
            .get(Protocol.MIN_READER_VERSION_PROP).map(_.toInt)
            .getOrElse(oldProtocol.minReaderVersion),
        minWriterVersion = caseInsensitiveMap
            .get(Protocol.MIN_WRITER_VERSION_PROP).map(_.toInt)
            .getOrElse(oldProtocol.minWriterVersion))

        if (!satisfyColumnMappingProtocol(newProtocol)) {
          throw changeColumnMappingModeOnOldProtocol(oldProtocol)
      }
    }

    tryFixMetadata(oldMetadata, newMetadata, isChangingModeOnExistingTable)
  }

  private def tryFixMetadata(
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isChangingModeOnExistingTable: Boolean): Metadata = {
    val newMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.fromMetadata(newMetadata)
    newMappingMode match {
      case IdMapping | NameMapping =>
        assignColumnIdAndPhysicalName(newMetadata, oldMetadata, isChangingModeOnExistingTable)
      case NoMapping =>
        newMetadata
      case mode =>
        throw unsupportedColumnMappingMode(mode.name)
    }
  }

  /**
   * Verify the ids and physical names assigned to the columns are unique and are valid.
   * If not throw errors.
   */
  def checkColumnIdAndPhysicalNameAssignments(
      schema: StructType,
      mode: DeltaColumnMappingMode): Unit = {
    // physical name/column id -> full field path
    val columnIds = mutable.Set[Int]()
    val physicalNames = mutable.Set[String]()

    // use id mapping to keep all column mapping metadata
    // this method checks for missing physical name & column id already
    val physicalSchema = createPhysicalSchema(schema, schema, IdMapping, checkSupportedMode = false)

    SchemaMergingUtils.transformColumns(physicalSchema) ((parentPhysicalPath, field, _) => {
      // field.name is now physical name
      // We also need to apply backticks to column paths with dots in them to prevent a possible
      // false alarm in which a column `a.b` is duplicated with `a`.`b`
      val curFullPhysicalPath = SchemaUtils.prettyFieldName(parentPhysicalPath :+ field.getName)
      val columnId = getColumnId(field)
      if (columnIds.contains(columnId)) {
        throw DeltaErrors.duplicatedColumnId(mode, columnId, schema)
      }
      columnIds.add(columnId)

      // We should check duplication by full physical name path, because nested fields
      // such as `a.b.c` shouldn't conflict with `x.y.c` due to same column name.
      if (physicalNames.contains(curFullPhysicalPath)) {
        throw DeltaErrors.duplicatedPhysicalName(mode, curFullPhysicalPath, schema)
      }
      physicalNames.add(curFullPhysicalPath)

      field
    })
  }

  /**
   * Create a physical schema for the given schema using the Delta table schema as a reference.
   * Physical schema is used when reading data from or writing data into parquet files.
   *
   * @param schema the given logical schema (potentially without any metadata)
   * @param referenceSchema the schema from the delta log, which has all the metadata
   * @param columnMappingMode column mapping mode of the delta table, which determines which
   *                          metadata to fill in
   * @param checkSupportedMode whether we should check of the column mapping mode is supported
   */
  def createPhysicalSchema(
      schema: StructType,
      referenceSchema: StructType,
      columnMappingMode: DeltaColumnMappingMode,
      checkSupportedMode: Boolean = true): StructType = {
    if (columnMappingMode == NoMapping) {
      return schema
    }

    // createPhysicalSchema is the narrow-waist for both read/write code path
    // so we could check for mode support here
    if (checkSupportedMode && !supportedModes.contains(columnMappingMode)) {
      throw DeltaErrors.unsupportedColumnMappingMode(columnMappingMode.name)
    }

    SchemaMergingUtils.transformColumns(schema) { (path, field, _) =>
      val fullName = path :+ field.getName
      val inSchema = SchemaUtils
        .findNestedFieldIgnoreCase(referenceSchema, fullName)
      inSchema.map { refField =>
        val physicalMetadata = getColumnMappingMetadata(refField, columnMappingMode)
        field.withNewName(getPhysicalName(refField))
          .withNewMetadata(physicalMetadata)
      }.getOrElse {
        if (isInternalField(field)) {
          field
        } else {
          throw DeltaErrors.columnNotFound(fullName, referenceSchema)
        }
      }
    }
  }

  def dropColumnMappingMetadata(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      field.withNewMetadata(
        FieldMetadata.builder()
          .withMetadata(field.getMetadata)
          .remove(COLUMN_MAPPING_METADATA_ID_KEY)
          .remove(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
          .remove(PARQUET_FIELD_ID_METADATA_KEY)
          .build()
        )
    }
  }

  /**
   * For each column/field in a Metadata's schema, assign id using the current maximum id
   * as the basis and increment from there, and assign physical name using UUID
   * @param newMetadata The new metadata to assign Ids and physical names
   * @param oldMetadata The old metadata
   * @param isChangingModeOnExistingTable whether this is part of a commit that changes the
   *                                      mapping mode on a existing table
   * @return new metadata with Ids and physical names assigned
   */
  private def assignColumnIdAndPhysicalName(
      newMetadata: Metadata,
      oldMetadata: Metadata,
      isChangingModeOnExistingTable: Boolean): Metadata = {
    val rawSchema = newMetadata.schema
    var maxId = DeltaConfigs.COLUMN_MAPPING_MAX_ID.fromMetadata(newMetadata) max
        findMaxColumnId(rawSchema)
    val newSchema =
      SchemaMergingUtils.transformColumns(rawSchema)((path, field, _) => {
        // Start with existing field metadata and add needed column mapping metadata
        val builder = FieldMetadata.builder().withMetadata(field.getMetadata())

        if (!hasColumnId(field)) {
          maxId += 1
          builder.putLong(COLUMN_MAPPING_METADATA_ID_KEY, maxId)
        }

        if (!hasPhysicalName(field)) {
          val physicalName = if (isChangingModeOnExistingTable) {
            val fullName = path :+ field.getName()
            val existingField = SchemaUtils.findNestedFieldIgnoreCase(oldMetadata.schema, fullName)
            if (existingField.isEmpty) {
              throw schemaChangeDuringMappingModeChangeNotSupported(
                oldMetadata.schema, newMetadata.schema)
            }
            // When changing from NoMapping to NameMapping mode, we directly use old display names
            // as physical names. This is by design: 1) We don't need to rewrite the
            // existing Parquet files, and 2) display names in no-mapping mode have all the
            // properties required for physical names: unique, stable and compliant with Parquet
            // column naming restrictions.
            existingField.get.getName()
          } else {
            generatePhysicalName
          }

          builder.putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
        }
        field.withNewMetadata(builder.build())
      })

    newMetadata.copy(
      schemaString = newSchema.toJson,
      configuration =
        newMetadata.configuration ++ Map(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> maxId.toString))
  }

  /**
   * Gets the required column metadata for each column based on the column mapping mode.
   */
  private def getColumnMappingMetadata(
      field: StructField, mode: DeltaColumnMappingMode): FieldMetadata = {
    mode match {
      case NoMapping =>
        // drop all column mapping related fields
        FieldMetadata.builder()
            .withMetadata(field.getMetadata)
            .remove(COLUMN_MAPPING_METADATA_ID_KEY)
            .remove(PARQUET_FIELD_ID_METADATA_KEY)
            .remove(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
            .build()

      case IdMapping =>
        if (!hasColumnId(field)) {
          throw DeltaErrors.missingColumnId(IdMapping, field.getName)
        }
        if (!hasPhysicalName(field)) {
          throw DeltaErrors.missingPhysicalName(IdMapping, field.getName)
        }
        FieldMetadata.builder()
            .withMetadata(field.getMetadata)
            .putLong(PARQUET_FIELD_ID_METADATA_KEY, getColumnId(field))
            .build()

      case NameMapping =>
        if (!hasPhysicalName(field)) {
          throw DeltaErrors.missingPhysicalName(NameMapping, field.getName)
        }
        FieldMetadata.builder()
            .withMetadata(field.getMetadata)
            .remove(COLUMN_MAPPING_METADATA_ID_KEY)
            .remove(PARQUET_FIELD_ID_METADATA_KEY)
            .build()

      case mode =>
        throw DeltaErrors.unsupportedColumnMappingMode(mode.name)
    }
  }

  /** Get the physical name of the column. If no physical name defined, return the logical name */
  def getPhysicalName(field: StructField): String = {
    if (field.getMetadata.contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY)) {
      field.getMetadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY).toString
    } else {
      field.getName
    }
  }

  /**
   * The only allowed mode change is from NoMapping to NameMapping or IdMapping. Other changes
   * would require re-writing Parquet files and are not supported right now.
   */
  private def allowMappingModeChange(
      oldMode: DeltaColumnMappingMode, newMode: DeltaColumnMappingMode): Boolean = {
    if (oldMode == newMode) true
    else (oldMode == NoMapping && newMode == NameMapping) ||
      (oldMode == NoMapping && newMode == IdMapping)
  }

  private def satisfyColumnMappingProtocol(protocol: Protocol): Boolean =
    protocol.minWriterVersion >= MIN_WRITER_VERSION &&
        protocol.minReaderVersion >= MIN_READER_VERSION

  private def findMaxColumnId(schema: StructType): Long = {
    var maxId: Long = 0
    SchemaMergingUtils.transformColumns(schema)((_, f, _) => {
      if (hasColumnId(f)) {
        maxId = maxId max getColumnId(f)
      }
      f
    })
    maxId
  }

  private def hasColumnId(field: StructField): Boolean =
    field.getMetadata().contains(COLUMN_MAPPING_METADATA_ID_KEY)

  private def getColumnId(field: StructField): Int =
    field.getMetadata().get(COLUMN_MAPPING_METADATA_ID_KEY).asInstanceOf[java.lang.Long].toInt

  private def hasPhysicalName(field: StructField): Boolean =
    field.getMetadata().contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY)

  def isInternalField(field: StructField): Boolean = DELTA_INTERNAL_COLUMNS
      .contains(field.getName.toLowerCase(Locale.ROOT))

  private def generatePhysicalName: String = "col-" + UUID.randomUUID()
}

object DeltaColumnMapping extends DeltaColumnMappingBase

/**
 * A trait for Delta column mapping modes.
 */
sealed trait DeltaColumnMappingMode {
  val name: String
}

/**
 * No mapping mode uses a column's display name as its true identifier to
 * read and write data.
 *
 * This is the default mode and is the same mode as Delta always has been.
 */
case object NoMapping extends DeltaColumnMappingMode {
  val name = "none"
}

/**
 * Id Mapping uses column ID as the true identifier of a column. Column IDs are stored as
 * StructField metadata in the schema and will be used when reading and writing Parquet files.
 * The Parquet files in this mode will also have corresponding field Ids for each column in their
 * file schema.
 */
case object IdMapping extends DeltaColumnMappingMode {
  val name = "id"
}

/**
 * Name Mapping uses the physical column name as the true identifier of a column. The physical name
 * is stored as part of StructField metadata in the schema and will be used when reading and writing
 * Parquet files. Even if id mapping can be used for reading the physical files, name mapping is
 * used for reading statistics and partition values in the DeltaLog.
 */
case object NameMapping extends DeltaColumnMappingMode {
  val name = "name"
}

object DeltaColumnMappingMode {
  def apply(name: String): DeltaColumnMappingMode = {
    name.toLowerCase(Locale.ROOT) match {
      case NoMapping.name => NoMapping
      case IdMapping.name => IdMapping
      case NameMapping.name => NameMapping
      case mode => throw DeltaErrors.unsupportedColumnMappingMode(mode)
    }
  }
}

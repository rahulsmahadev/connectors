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

// scalastyle:off
import com.github.mjakubowski84.parquet4s.ParquetReader
import io.delta.standalone.internal.actions.{AddFile, CustomJsonIterator, CustomParquetIterator, Parquet4sSingleActionWrapper, SingleAction}
import io.delta.standalone.internal.util.{FileNames, JsonUtils}
import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.fs.Path
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.impl.{UnionListReader, UnionMapReader}
import org.apache.arrow.vector.holders.{RepeatedListHolder, VarCharHolder}

import scala.io.Source
import scala.collection.JavaConverters._
import java.io.File
import java.util.Arrays.asList
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ArrowConversionUtils {

  def convertFieldVectorToAddFile(vector: StructVector, index: Int): AddFile = {
    val path = vector
      .getChild("path").asInstanceOf[VarCharVector]
      .getObject(index)

    val modificationTime = vector
      .getChild("modificationTime").asInstanceOf[BigIntVector]
      .getObject(index)

    val size = vector
      .getChild("size").asInstanceOf[BigIntVector]
      .getObject(index)

    val dataChange = vector
      .getChild("dataChange").asInstanceOf[BitVector]
      .getObject(index)

    val statsText = vector
      .getChild("stats").asInstanceOf[VarCharVector]
      .getObject(index)

    val stats = if (statsText == null) {
      null
    } else {
      statsText.toString
    }

    //TODO map type

    AddFile(
      path.toString,
      Map.empty,
      size,
      modificationTime,
      dataChange,
      stats,
      Map.empty
    )

  }

}

/**
 * Helper singleton to create Arrow schema for the corresponding items.
 */
object ArrowDeltaSchemaUtils {

  def stringField(name: String): Field = {
    new Field(
      name,
      new FieldType(true, new ArrowType.Utf8, null),
      null
    )
  }

  def listField(name: String): Field = {
    new Field(
      name,
      new FieldType(false, new ArrowType.List, null),
      Seq(stringField(name)).asJava
    )
  }

  def mapField(): Field = {
    new Field(
      "struct",
      FieldType.notNullable(new ArrowType.Struct),
      Seq(listField(MapVector.KEY_NAME), listField(MapVector.VALUE_NAME)).asJava
    )
  }

  def getAddFileSchema(): Field = {

    val pathField = new Field(
      "path",
      FieldType.notNullable(new ArrowType.Utf8()),
      null)

    val partitionValuesField = new Field(
      "partitionValues",
      FieldType.nullable(new ArrowType.Map(false)),
      Seq(mapField()).asJava
    )
    val sizeField = new Field(
      "size",
      FieldType.nullable(new ArrowType.Int(64, true)),
      null
    )
    val modificationTimeField = new Field(
      "modificationTime",
      FieldType.notNullable(new ArrowType.Int(64, true)),
      null
    )
    val dataChangeField = new Field(
      "dataChange",
      FieldType.nullable(new ArrowType.Bool()),
      null
    )
    val statsField = new Field(
      "stats",
      FieldType.nullable(new ArrowType.Utf8()),
      null)

    val tagsField = new Field(
      "tags",
      FieldType.notNullable(new ArrowType.Map(false)),
      Seq(mapField()).asJava)

    val addField = new Field(
      "add",
      FieldType.nullable(new ArrowType.Struct),
      Seq(pathField, partitionValuesField, sizeField, modificationTimeField, dataChangeField, statsField, tagsField)
        .asJava
    )
    addField
  }

  def getRemoveFileSchema(): Field = {

    val pathField = new Field(
      "path",
      FieldType.notNullable(new ArrowType.Utf8()),
      null)

    val deletionTimeField = new Field(
      "deletionTimestamp",
      FieldType.nullable(new ArrowType.Int(64, true)),
      null
    )
    val dataChangeField = new Field(
      "dataChange",
      FieldType.nullable(new ArrowType.Bool()),
      null
    )
    val extendedFileMetadataField = new Field(
      "extendedFileMetadata",
      FieldType.nullable(new ArrowType.Bool()),
      null
    )

    val tagsField = new Field(
      "tags",
      FieldType.notNullable(new ArrowType.Map(false)),
      Seq(mapField()).asJava)

    val partitionValuesField = new Field(
      "partitionValues",
      FieldType.nullable(new ArrowType.Map(false)),
      Seq(mapField()).asJava
    )
    val sizeField = new Field(
      "size",
      FieldType.nullable(new ArrowType.Int(64, true)),
      null
    )
    val removeField = new Field(
      "remove",
      FieldType.nullable(new ArrowType.Struct),
      Seq(
        pathField,
        deletionTimeField,
        dataChangeField,
        extendedFileMetadataField,
        tagsField,
        partitionValuesField,
        sizeField
      ).asJava
    )
    removeField
  }
  def getActionSchema(): Schema = {
    new Schema(asList(getAddFileSchema(), getRemoveFileSchema()), null)
  }
}


/**
 * This is the base implementation of the compute engine, we will read the parquet and
 *
 * The plan is the calling engine will override these methods with engine optimized implementations.
 */
class ComputeEngine {

  /* Need an off-heap memory allocator */
  val allocator = new RootAllocator(Int.MaxValue)

  val arrowBufferTrackers = new ArrayBuffer[VectorSchemaRoot]()

  /**
   * Called by Delta-Core when its done using the compute engine
   */
  def close(): Unit = {
    arrowBufferTrackers.foreach { buf =>
      buf.close()
    }
  }

  private def convertAddFileToArrow(index: Int, singleAction: SingleAction, addVector: StructVector): Unit = {
    if (singleAction.add != null) {
      val add = singleAction.add
      addVector.getChild("path").asInstanceOf[VarCharVector].setSafe(index, add.path.getBytes)
      addVector.getChild("size").asInstanceOf[BigIntVector].setSafe(index, add.size)
      addVector.getChild("modificationTime").asInstanceOf[BigIntVector]
        .setSafe(index, add.modificationTime)
      val dataChange = if (add.dataChange) 1 else 0
      addVector.getChild("dataChange").asInstanceOf[BitVector].setSafe(index, dataChange)

      if (add.stats != null) {
        addVector.getChild("stats").asInstanceOf[VarCharVector]
          .setSafe(index, add.stats.getBytes)
      }

      // TODO write methods for every type to reduce code duplication
      val partitionValuesVector = addVector.getChild("partitionValues").asInstanceOf[MapVector]
      val structVector = partitionValuesVector.getDataVector.asInstanceOf[StructVector]
      structVector.allocateNew()
      val keysVector = structVector.getChild(MapVector.KEY_NAME).asInstanceOf[ListVector]
      val valuesVector = structVector.getChild(MapVector.VALUE_NAME).asInstanceOf[ListVector]


      partitionValuesVector.startNewValue(index)
      keysVector.allocateNew()
      valuesVector.allocateNew()
      add.partitionValues.zipWithIndex.foreach { case (kv, mapIndex) =>
        keysVector.startNewValue(mapIndex)
        valuesVector.startNewValue(mapIndex)

        structVector.setIndexDefined(mapIndex)
        keysVector.getDataVector.asInstanceOf[VarCharVector].setSafe(
          mapIndex, kv._1.getBytes
        )
        valuesVector.getDataVector.asInstanceOf[VarCharVector].setSafe(
          mapIndex, kv._2.getBytes
        )
      }
      keysVector.endValue(index, add.partitionValues.size)
      valuesVector.endValue(index, add.partitionValues.size)

      partitionValuesVector.endValue(index, add.partitionValues.size)
      // TODO - copy over tags as well
    } else {
      addVector.setNull(index)
    }
  }

  private def convertRemoveFileToArrow(index: Int, singleAction: SingleAction, removeVector: StructVector): Unit = {
    if (singleAction.remove != null) {
      val remove = singleAction.remove
      removeVector.getChild("path").asInstanceOf[VarCharVector].setSafe(index, remove.path.getBytes)
      removeVector.getChild("deletionTimestamp").asInstanceOf[BigIntVector]
        .setSafe(index, remove.delTimestamp)

      val dataChange = if (remove.dataChange) 1 else 0
      removeVector.getChild("dataChange").asInstanceOf[BitVector].setSafe(index, dataChange)

      val extendedMetadata = if (remove.extendedFileMetadata) 1 else 0
      removeVector.getChild("extendedFileMetadata").asInstanceOf[BitVector].setSafe(index, extendedMetadata)

      if (remove.size.nonEmpty) {
        removeVector.getChild("size").asInstanceOf[BigIntVector].setSafe(index, remove.size.get)
      }

      if (remove.partitionValues != null) {
        val partitionValuesVector = removeVector.getChild("partitionValues").asInstanceOf[MapVector]
        val structVector = partitionValuesVector.getDataVector.asInstanceOf[StructVector]
        structVector.allocateNew()
        val keysVector = structVector.getChild(MapVector.KEY_NAME).asInstanceOf[ListVector]
        val valuesVector = structVector.getChild(MapVector.VALUE_NAME).asInstanceOf[ListVector]

        partitionValuesVector.startNewValue(index)
        keysVector.allocateNew()
        valuesVector.allocateNew()
        remove.partitionValues.zipWithIndex.foreach { case (kv, mapIndex) =>
          keysVector.startNewValue(mapIndex)
          valuesVector.startNewValue(mapIndex)

          structVector.setIndexDefined(mapIndex)
          keysVector.getDataVector.asInstanceOf[VarCharVector].setSafe(
            mapIndex, kv._1.getBytes
          )
          valuesVector.getDataVector.asInstanceOf[VarCharVector].setSafe(
            mapIndex, kv._2.getBytes
          )
        }
        keysVector.endValue(index, remove.partitionValues.size)
        valuesVector.endValue(index, remove.partitionValues.size)

        partitionValuesVector.endValue(index, remove.partitionValues.size)
      }
      // TODO - copy over tags as well
    } else {
      removeVector.setNull(index)
    }
  }

  private def convertSingleActionsToArrow(actions: Seq[SingleAction]): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(
      ArrowDeltaSchemaUtils.getActionSchema(),
      allocator)

    // we need to set row count beforehand
    root.setRowCount(actions.size)

    /* we need to track all the buffers we allocate to make sure we clear them after use.  */
    arrowBufferTrackers.append(root)

    val addVector = root.getVector("add").asInstanceOf[StructVector]
    val removeVector = root.getVector("remove").asInstanceOf[StructVector]

    addVector.allocateNew()
    removeVector.allocateNew() // TODO figure out if this is required

    /** Supporting only AddFile and RemoveFile now */
    val supportedActions = actions
      .filter(action => (action.add != null || action.remove != null))

    supportedActions.zipWithIndex.foreach { case (action, index) =>
      convertAddFileToArrow(
        index,
        action,
        addVector
      )

      convertRemoveFileToArrow(
        index,
        action,
        removeVector
      )
    }
    root
  }

  def readJsonAsArrow(path: Path): VectorSchemaRoot = {
    // HACK: removing the fake: from the path.
    val strippedPath = new File(path.toString.substring(5))
    println(s"Reading file ${strippedPath} as an arrow table")

    val actions = Source.fromFile(strippedPath).getLines().map { line =>
      JsonUtils.mapper.readValue[SingleAction](line)
    }.toSeq

    actions.foreach {
      case single: SingleAction if single.add != null =>
        println("add file")
      case single: SingleAction if single.remove != null =>
        println("Remove")
      case _ =>
        println("Other")
    }
    convertSingleActionsToArrow(actions)
  }

  def readParquetAsArrow(path: Path): VectorSchemaRoot = {
    // HACK: removing the fake: from the path.
    val strippedPath = new File(path.toString.substring(5))
    println(s"Reading file ${strippedPath} as an arrow table")
    val parquetIterable = ParquetReader.read[Parquet4sSingleActionWrapper](
      strippedPath.toString,
      ParquetReader.Options()
    )
    // TODO copy other type of actiosn
    val actions = parquetIterable.flatMap {
      case sa: Parquet4sSingleActionWrapper if sa.add != null =>
        Some(sa.add.wrap)
      case _ =>
        None
    }
    actions.foreach {
      case single: SingleAction if single.add != null =>
        println("add file")
      case single: SingleAction if single.remove != null =>
        println("Remove")
      case _ =>
        println("Other")
    }
    convertSingleActionsToArrow(actions.toSeq)
  }
}

private[internal] class SnapshotArrowImpl(
    override val hadoopConf: Configuration,
    override val path: Path,
    override val version: Long,
    override val logSegment: LogSegment,
    override val minFileRetentionTimestamp: Long,
    override val deltaLog: DeltaLogImpl,
    override val timestamp: Long,
    computeEngine: ComputeEngine = new ComputeEngine()) extends SnapshotImpl(
  hadoopConf, path, version, logSegment, minFileRetentionTimestamp, deltaLog, timestamp) {

    trait LogFileFormat

    case class JsonLogFile() extends LogFileFormat

    case class ParquetLogFile() extends LogFileFormat

    /** Keep track of remove files until we see the corresponding AddFile */
    var seenSet: mutable.Set[String] = mutable.Set()

    private def replayFile(
        path: Path,
        fmt: LogFileFormat): VectorSchemaRoot = {
      val (vsr: VectorSchemaRoot, trackRemoves: Boolean) = fmt match {
        case j: JsonLogFile =>
          (computeEngine.readJsonAsArrow(path), true)
        case p: ParquetLogFile =>
          (computeEngine.readParquetAsArrow(path), false)
      }
      println(s"replaying file ${path}")
      println("printing add files")
      println("--------------------")
      for (i <- 0 to vsr.getRowCount) {
        val addPath = vsr.getVector("add").asInstanceOf[StructVector]
          .getChild("path").asInstanceOf[VarCharVector]
          .getObject(i)

        if (addPath != null) {
          if (!seenSet.contains(addPath.toString)) {
            println(s"valid add file ${addPath}")
          } else {
            println(s"removed file ${addPath}")
            // We can delete the row from arrow since this file is no longer active
            // TODO: Figure out how to set all fields to null
            vsr.getVector("add").asInstanceOf[StructVector]
              .getChild("path")
              .asInstanceOf[VarCharVector]
              .setNull(i)
            seenSet.remove(addPath.toString)
          }

        }
      }
      println("--------------------")
      println("printing remove files")
      println("--------------------")
      for (i <- 0 to vsr.getRowCount) {
        val removePath = vsr.getVector("remove").asInstanceOf[StructVector]
          .getChild("path").asInstanceOf[VarCharVector]
          .getObject(i)
        if (removePath != null && trackRemoves) {
          seenSet.add(removePath.toString)
        }
      }
      println("--------------------")
      vsr
    }

    private def arrowLogReplay(
        files: Seq[Path],
        filter: Option[String] = None): Seq[VectorSchemaRoot] = {

      files.reverse.map { file =>
        if (FileNames.isDeltaFile(file)) {
          replayFile(file, JsonLogFile())
        } else if (FileNames.isCheckpointFile(file)) {
          replayFile(file, ParquetLogFile())
        } else {
          throw new Exception(s"Unexpected file format for file - ${file}")
        }
      }
    }

    override lazy val state: SnapshotImpl.State = {
      val files = super.files

      val activeFiles: Seq[AddFile] = try {
        val vsrBatches = arrowLogReplay(files, None)
        vsrBatches.flatMap { batch =>
          (for (i <- 0 until batch.getRowCount) yield i).flatMap { index =>
            val path = batch.getVector("add")
              .asInstanceOf[StructVector]
              .getChild("path").asInstanceOf[VarCharVector]
              .getObject(index)
            if (path != null) {
              Some(ArrowConversionUtils.convertFieldVectorToAddFile(
                batch.getVector("add").asInstanceOf[StructVector], index))
            } else {
              None
            }
          }
        }
      } finally {
        computeEngine.close()
      }

      println(activeFiles)

      SnapshotImpl.State(
        Seq.empty,
        activeFiles,
        Seq.empty,
        0L,
        0L,
        0L,
        0L
      )
    }

}

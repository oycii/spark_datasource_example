package org.example.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = PostgresTable.schema

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = new PostgresTable(properties.get("tableName")) // TODO: Error handling
}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new PostgresScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType().add("user_id", LongType)
}

case class ConnectionProperties(url: String, user: String, password: String, tableName: String, partitionSize: Int)

/** Read */

class PostgresScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new PostgresScan(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName"), options.get("partitionSize").toInt
  ))
}

class PostgresPartition(val from: Long, val to: Long) extends InputPartition {
  override def toString: String = super.toString + ", from: " + from + ", to: " + to
}

class PostgresScan(connectionProperties: ConnectionProperties) extends Scan with Batch {
  override def readSchema(): StructType = PostgresTable.schema

  override def toBatch: Batch = this



  override def planInputPartitions(): Array[InputPartition] = {
    val countRecords = DefaultSourceHelper.getTableCount(connectionProperties)
    countRecords match {
      case Some(count) => {
        if (connectionProperties.partitionSize == 0) {
          Array(new PostgresPartition(-1, -1))
        } else {
          val countPartitions: Long = count / connectionProperties.partitionSize
          var from = 0L
          val listBuffer: mutable.ListBuffer[PostgresPartition] = ListBuffer[PostgresPartition]()

          for (i <- 0L to  countPartitions - 1) {
            val to = if (i == (countPartitions - 1)) count else (from + connectionProperties.partitionSize - 1)
            val postgresPartition = new PostgresPartition(from, to)
            listBuffer += postgresPartition
            from = to + 1
          }

          if (from < count) {
            val p = new PostgresPartition(from, count)
            listBuffer += p
          }

          listBuffer.toArray
        }
      }
      case None => Array(new PostgresPartition(-1, -1))
    }

  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PostgresPartitionReaderFactory(connectionProperties)
  }
}

class PostgresPartitionReaderFactory(connectionProperties: ConnectionProperties)
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p :PostgresPartition = partition.asInstanceOf[PostgresPartition]
    new PostgresPartitionReader(connectionProperties, p)
  }
}

class PostgresPartitionReader(connectionProperties: ConnectionProperties, p :PostgresPartition) extends PartitionReader[InternalRow] {
  private val connection: Connection = DefaultSourceHelper.getConnection(connectionProperties)

  private val statement = connection.createStatement()
  private val resultSet = {
    statement.executeQuery(s"select * from ${connectionProperties.tableName} LIMIT ${p.to - p.from + 1} OFFSET ${p.from} ")
  }

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getLong(1))

  override def close(): Unit = connection.close()
}

/** Write */

class PostgresWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName"), options.get("partitionSize").toInt
  ))
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] =
    new PostgresWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  val connection = DefaultSourceHelper.getConnection(connectionProperties)
  val statement = "insert into users (user_id) values (?)"
  val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}

object DefaultSourceHelper {
  def getConnection(connectionProperties: ConnectionProperties): Connection = {
    DriverManager.getConnection(
      connectionProperties.url, connectionProperties.user, connectionProperties.password
    )
  }

  def getTableCount(connectionProperties: ConnectionProperties): Option[Long] = {
    val con = getConnection(connectionProperties)
    try {
      val statement = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery("select count(1) from " + connectionProperties.tableName)
      rs.next()
      Some(rs.getLong(1))
    } catch {
      case e: Throwable => e.printStackTrace
        None
      case _ =>  None
    } finally {
      con.close()
    }
  }
}


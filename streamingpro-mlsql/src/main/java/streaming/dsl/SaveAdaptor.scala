/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import streaming.core.datasource.{DataSinkConfig, DataSourceRegistry, MLSQLSink}
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SaveAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {

    var oldDF: DataFrame = null
    var mode = SaveMode.ErrorIfExists
    var final_path = ""
    var format = ""
    var option = Map[String, String]()
    var tableName = ""
    var partitionByCol = ArrayBuffer[String]()

    val owner = option.get("owner")

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
          format match {
            case "hive" =>
            case _ =>
              format = s.getText
          }


        case s: PathContext =>
          format match {
            case "hive" | "kafka8" | "kafka9" | "hbase" | "redis" | "es" | "jdbc" =>
              final_path = cleanStr(s.getText)
            case "parquet" | "json" | "csv" | "orc" =>
              final_path = withPathPrefix(scriptSQLExecListener.pathPrefix(owner), cleanStr(s.getText))
            case _ =>
              final_path = cleanStr(s.getText)
          }

          final_path = TemplateMerge.merge(final_path, scriptSQLExecListener.env().toMap)

        case s: TableNameContext =>
          tableName = evaluate(s.getText)
          oldDF = scriptSQLExecListener.sparkSession.table(tableName)
        case s: OverwriteContext =>
          mode = SaveMode.Overwrite
        case s: AppendContext =>
          mode = SaveMode.Append
        case s: ErrorIfExistsContext =>
          mode = SaveMode.ErrorIfExists
        case s: IgnoreContext =>
          mode = SaveMode.Ignore
        case s: ColContext =>
          partitionByCol += cleanStr(s.identifier().getText)
        case s: ColGroupContext =>
          partitionByCol += cleanStr(s.col().identifier().getText)
        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case _ =>
      }
    }

    if (scriptSQLExecListener.env().contains("stream")) {
      new StreamSaveAdaptor(scriptSQLExecListener, option, oldDF, final_path, tableName, format, mode, partitionByCol.toArray).parse
    } else {
      new BatchSaveAdaptor(scriptSQLExecListener, option, oldDF, final_path, tableName, format, mode, partitionByCol.toArray).parse
    }
    scriptSQLExecListener.setLastSelectTable(null)

  }
}

class BatchSaveAdaptor(val scriptSQLExecListener: ScriptSQLExecListener,
                       var option: Map[String, String],
                       var oldDF: DataFrame,
                       var final_path: String,
                       var tableName: String,
                       var format: String,
                       var mode: SaveMode,
                       var partitionByCol: Array[String]
                      ) {
  def parse = {

    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
    }
    var writer = oldDF.write
    DataSourceRegistry.fetch(format).map { datasource =>
      datasource.asInstanceOf[MLSQLSink].save(
        writer,
        DataSinkConfig(final_path, option ++ Map("partitionByCol" -> partitionByCol.mkString(",")),
          mode))
    }.getOrElse {
      val dbAndTable = final_path.split("\\.")
      var connect_provied = false
      if (dbAndTable.length == 2 && ScriptSQLExec.dbMapping.containsKey(dbAndTable(0))) {
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach {
          f =>
            writer.option(f._1, f._2)
        }
        connect_provied = true
      }

      if (connect_provied) {
        final_path = dbAndTable(1)
      }


      writer = writer.format(format).mode(mode).partitionBy(partitionByCol: _*).options(option)

      def getKafkaBrokers = {
        "metadata.broker.list" -> option.getOrElse("metadata.broker.list", "kafka.bootstrap.servers")
      }

      format match {
        case "es" =>
          writer.format(
            option.getOrElse("implClass", "org.elasticsearch.spark.sql")).save(final_path)
        case "hive" =>
          writer.format(option.getOrElse("file_format", "parquet"))
          writer.saveAsTable(final_path)

        case "kafka8" | "kafka9" =>

          writer.option("topics", final_path).
            option(getKafkaBrokers._1, getKafkaBrokers._2).
            format("com.hortonworks.spark.sql.kafka08").save()

        case "kafka" =>
          writer.option("topic", final_path).
            option(getKafkaBrokers._1, getKafkaBrokers._2).format("kafka").save()

        case "hbase" =>
          writer.option("outputTableName", final_path).format(
            option.getOrElse("implClass", "org.apache.spark.sql.execution.datasources.hbase")).save()
        case "redis" =>
          writer.option("outputTableName", final_path).format(
            option.getOrElse("implClass", "org.apache.spark.sql.execution.datasources.redis")).save()
        case "jdbc" =>
          if (option.contains("idCol")) {
            import org.apache.spark.sql.jdbc.DataFrameWriterExtensions._
            val extraOptionsField = writer.getClass.getDeclaredField("extraOptions")
            extraOptionsField.setAccessible(true)
            val extraOptions = extraOptionsField.get(writer).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
            val jdbcOptions = new JDBCOptions(extraOptions.toMap + ("dbtable" -> final_path))
            writer.upsert(option.get("idCol"), jdbcOptions, oldDF)
          } else {
            writer.option("dbtable", final_path).save()
          }

        case "carbondata" =>
          if (dbAndTable.size == 2) {
            writer.option("tableName", dbAndTable(1)).option("dbName", dbAndTable(0))
          }
          if (dbAndTable.size == 1 && dbAndTable(0) != "-") {
            writer.option("tableName", dbAndTable(0))
          }
          writer.format(option.getOrElse("implClass", "org.apache.spark.sql.CarbonSource")).save()
        case _ =>
          if (final_path == "-" || final_path.isEmpty) {
            writer.format(option.getOrElse("implClass", format)).save()
          } else {
            writer.format(option.getOrElse("implClass", format)).save(final_path)
          }

      }
    }

  }
}

class StreamSaveAdaptor(val scriptSQLExecListener: ScriptSQLExecListener,
                        var option: Map[String, String],
                        var oldDF: DataFrame,
                        var final_path: String,
                        var tableName: String,
                        var format: String,
                        var mode: SaveMode,
                        var partitionByCol: Array[String]
                       ) {
  def parse = {
    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
    }

    var writer: DataStreamWriter[Row] = oldDF.writeStream
    val dbAndTable = final_path.split("\\.")
    var connect_provied = false
    if (dbAndTable.length == 2 && ScriptSQLExec.dbMapping.containsKey(dbAndTable(0))) {
      ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach {
        f =>
          writer.option(f._1, f._2)
      }
      connect_provied = true
    }

    if (connect_provied) {
      final_path = dbAndTable(1)
    }


    require(option.contains("checkpointLocation"), "checkpointLocation is required")
    require(option.contains("duration"), "duration is required")
    require(option.contains("mode"), "mode is required")

    format match {
      case "jdbc" => writer.format("org.apache.spark.sql.execution.streaming.JDBCSinkProvider")
      /*
      Supports variable in path:
        save append post_parquet
        as parquet.`/post/details/hp_stat_date=${date.toString("yyyy-MM-dd")}`
       */
      case "newParquet" => writer.format("org.apache.spark.sql.execution.streaming.newfile")
      case _ => writer.format(option.getOrElse("implClass", format))
    }

    writer = writer.outputMode(option("mode")).
      partitionBy(partitionByCol: _*).
      options((option - "mode" - "duration"))

    val dbtable = if (option.contains("dbtable")) option("dbtable") else final_path

    if (dbtable != null && dbtable != "-") {
      writer.option("path", dbtable)
    }
    scriptSQLExecListener.env().get("streamName") match {
      case Some(name) => writer.queryName(name)
      case None =>
    }
    writer.trigger(Trigger.ProcessingTime(option("duration").toInt, TimeUnit.SECONDS)).start()
  }
}

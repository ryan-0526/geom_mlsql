package tech.mlsql.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * Created by Lenovo on 2020/12/9.
  */
class SQLTableSchemaExt extends SQLAlg with WowParams {

  override val uid: String = BaseParams.randomUID()

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._
    val columnScheam: Array[(String, String)] = df.dtypes
    //增加时表结构
    if ("insert".equals(params.getOrElse(schemaType.name, " "))) {
      var columnValue = "";
      for (mark <- 0 until columnScheam.length) {
        columnValue += "a." + columnScheam(mark)._1 + ","
      }
      return spark.sparkContext.makeRDD(Seq(columnValue)).toDF()
      //修改时拼接表结构
    } else if ("update".equals(params.getOrElse(schemaType.name, " "))) {
      var columnValue = "and ("
      for (mark <- 0 until columnScheam.length) {
        var template1 = "NVL(a." + columnScheam(mark)._1 + ",now())<>NVL(b." + columnScheam(mark)._1 + ",now())"
        var template2 = "NVL(a." + columnScheam(mark)._1 + ",999999999)<>NVL(b." + columnScheam(mark)._1 + ",999999999)"
        if (mark < columnScheam.length - 1) {
          if (columnScheam(mark)._2.equals("DateType") || columnScheam(mark)._2.equals("TimestampType")) {
            columnValue += template1 + " or "
          } else {
            columnValue += template2 + " or "
          }
        } else {
          if (columnScheam(mark)._2.equals("DateType") || columnScheam(mark)._2.equals("TimestampType")) {
            columnValue += template1
          } else {
            columnValue += template2
          }
        }
      }
      columnValue += ")"
      return spark.sparkContext.makeRDD(Seq(columnValue)).toDF()
    } else {
      if (params.size == 0) {
        throw new MLSQLException(s"${schemaType.name} is required")
      } else {
        throw new MLSQLException(s"${schemaType.name} value is insert or update")
      }

    }
  }

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |When operating on data in a table, the SQLTableSchemaExt is used to get the names of the fields in the table to operate on
      |
      |insert the SQL
      |```sql
      |run tableName as TableSchemaExt.`` where type="insert" as AsName;
      |```
      |
      |gets the update comparison field the SQL
      |
      |```sql
      |run tableName as TableSchemaExt.`` where type="update" as AsName;
      |```
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |
      |run insert sql
      |```sql
      |run ODS_T_DRUG_DOSAGE_UNIT as TableSchemaExt.`` where type="insert" as AsName;
      |
      |```
      |Here are the result:
      |```
      |a.DRUG_DOSAGE_UNIT,a.IS_DEFAULT,a.RECORD_STATE,
      |```
      |
      |
      |run update sql
      |```sql
      |run ODS_T_DRUG_DOSAGE_UNIT as TableSchemaExt.`` where type="update" as AsName;
      |```
      |Here are the result:
      |```
      |and (NVL(a.DRUG_DOSAGE_UNIT,999999999)<>NVL(b.DRUG_DOSAGE_UNIT,999999999) or NVL(a.IS_DEFAULT,999999999)<>
      |NVL(b.IS_DEFAULT,999999999) or NVL(a.RECORD_STATE,999999999)<>NVL(b.RECORD_STATE,999999999))
      |```
      |
      |
      |
      |Query or insert in the table more fields, write out the SQL appears bloated, you can use RUN statement
      |to get all the fields, and set into a variable, can be used in SELECT
      |```
      |run ODS_T_DRUG_DOSAGE_UNIT as TableSchemaExt.`` where type="insert" as AsName;
      |set col_array=`select * from AsName` where type="sql" and mode="runtime";
      |select ${col_array} now() as create_time from ODS_T_DRUG_DOSAGE_UNIT as table1;
      |```
      |
      |
      |You can set the WHERE condition to UPDATE when updating the comparison table
      |```
      |run ODS_T_DRUG_DOSAGE_UNIT as TableSchemaExt.`` where type="update" as `${compare_col}`;
      |set up_col_array=`select * from T_DRUG_DOSAGE_UNIT` where type="sql" and mode="runtime";
      |select
      |id,
      |${col_array}
      |getPinYin(a.DRUG_DOSAGE_UNIT) as  phonetic_code,
      |getPinYin(a.DRUG_DOSAGE_UNIT) as  custom_code,
      |getPinYin(a.DRUG_DOSAGE_UNIT) as  code,
      |current_timestamp update_time
      |from T_DRUG_DOSAGE_UNIT b,ODS_T_DRUG_DOSAGE_UNIT a where 1=1 and b.DRUG_DOSAGE_UNIT=a.DRUG_DOSAGE_UNIT
      |${up_col_array}
      |as `${up_table_name}`;
      |```
      |
      |"""
    .stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  def spellStr(flag: Boolean): Unit = {

  }

  final val schemaType: Param[String] = new Param[String](this, "type",
    s"""type""")

}

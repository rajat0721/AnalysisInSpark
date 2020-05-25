package com.spark.dev


import org.apache.spark.sql.functions._
import com.spark.utility.SparkUtility
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.Concat

object ExplodeDF {

  val map1 = udf(mapper1)
  val map2 = udf(mapper2)

  def main(args: Array[String]): Unit = {

    val spark = SparkUtility.getSparkSession;

    /*val df1 = List("a,b","1,2").toDF("Col1")
    df1.show()*/

    val values = List(List("a,b", "1,2") ,List("b,c", "3,4") ,List("e,f", "5,6")).map(x =>(x(0), x(1)))
    import spark.implicits._
    val df = values.toDF()
    println("Input DF")
    df.show()
    //df = df.flatMap(_."col1".split(","))
    println("Output DF")
    val disDF = df.withColumn("merge1",concat($"_1",lit("#"),$"_2"))
      .withColumn("op1",map1($"merge1"))
        .select("op1").withColumn("op2",split($"op1", "#"))
        .withColumn("op3",explode($"op2"))
      .withColumn("op4", split($"op3", "\\|"))
      .withColumn("op5",$"op4".getItem(0))
      .withColumn("op6",$"op4".getItem(1))
    /*.select(
      $"op4".getItem(0).as("op5"),
      $"op4".getItem(1).as("op6")
    )*/



    disDF.show()

    val df1 = df.withColumn("array1",split($"_1", ",").getItem(0))
      .withColumn("array2",split($"_2", ","))
      .select( col("*") +:
        (0 until 3 ).map(i => (col("array1").getItem(i)+col("array2").getItem(i)).as(s"col$i")): _*
      )
        //.select("array1","array2")


   // val df2 = df1.withColumn("exploded",explode($"array1"))
      //.withColumn("exploded2",explode($"array2"))

    //df1.show()

  }

  def mapper1: (String => String) = { l =>

    val a = l.split("#")
    val col1 = a(0).split(",")
    val col2 = a(1).split(",")

    col1(0)+"|"+col2(0) + "#" +col1(1)+"|"+col2(1)
  }

  def mapper2: (String => String) = { l =>

    val a = l.split("#")
    val col1 = a(0).split(",")
    val col2 = a(1).split(",")

    col1(1)+","+col2(1)
  }

  /* val ds: Dataset[Book]

   val allWords = ds.select('title, explode(split('words, " ")).as("word"))

   val bookCountPerWord = allWords.groupBy("word").agg(countDistinct("title"))

Using flatMap() this can similarly be exploded as:

   ds.flatMap(_.words.split(" "))*/


}

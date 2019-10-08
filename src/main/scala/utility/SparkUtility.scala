package utility

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtility {

  def getSparkSession: SparkSession ={
    SparkSession.builder()
      .config("spark.master", "yarn")
      .getOrCreate()
  }

  def init():SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    getSparkSession
  }

  //In metadata last column was named as emailid but in explanation it was termed as mobile_number.
  def getAadharDF(spark:SparkSession):DataFrame={
    spark.read.option("inferSchema", true).csv("gs://dataproc-1e27335a-b915-40d8-b2c4-8b0cb9b83e41-asia-northeast2/Rajat/aadhaar_data.csv")
      .toDF("date","registrar","private_agency","state","district","sub_district",
        "pincode","gender","age","aadhaar_generated","rejected","emailid","mobile_number")
  }

}

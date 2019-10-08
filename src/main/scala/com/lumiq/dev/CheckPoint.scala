package com.lumiq.dev

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CheckPoint {


  def checkpoint1(spark: SparkSession, AadharDF: DataFrame) = {

    println("\n Initialising checkpoint1 \n")
    val private_agency = AadharDF.select("private_agency").collect().distinct.map(_.getString(0)) //.mkString(",")
    private_agency.foreach(i => {
      println(s"View for private_agency = $i")
      AadharDF.where(col("private_agency") === i)
        .show(4, false)
    })
  }

  def checkpoint2(spark: SparkSession, AadhaarDF: DataFrame) = {

    println("\n Initialising checkpoint2 \n")
    println("Describe Schema")
    AadhaarDF.printSchema()

    println("count of distinct registrar")
    val count_registrar_df = AadhaarDF.groupBy("registrar")
      .agg(count("registrar").as("count_registrar"))
    count_registrar_df.show(false)

    println("Number of states, districts in each state and sub-districts in each district.")
    println(s"Number of states = ${AadhaarDF.select("state").distinct.count()}")
    AadhaarDF.groupBy("state").agg(count("district").as("districts_in_each_state")).show(false)
    AadhaarDF.groupBy("district").agg(count("sub_district").as("sub_districts_in_each_district")).show(false)

    println("The names of private agencies for each state")
    AadhaarDF.groupBy("state").agg(collect_set("private_agency").as("name_private_agency_for_each_state"))
      //.withColumn("name_private_agency_for_each_state", concat_ws(",", col("name_private_agency_for_each_state")))
      .show(false)
  }

  def checkpoint3(spark: SparkSession, AadhaarDF: DataFrame) = {

    println("\n Initialising checkpoint3 \n")
    val w_state = Window.orderBy(desc("total_num_aadhaar_by_state"))
    val num_of_aadhar_generated_in_each_state = AadhaarDF.groupBy("state").agg(sum("aadhaar_generated").as("total_num_aadhaar_by_state"))
    val top_3_states_generated_most_aadhaar = num_of_aadhar_generated_in_each_state.withColumn("rank", dense_rank().over(w_state)).filter(col("rank") <= 3)
    top_3_states_generated_most_aadhaar.show(false)

    val w_district = Window.orderBy(desc("total_num_aadhaar_Enrollment_by_state"))
    val num_of_aadhar_enrollment_in_each_district = AadhaarDF.groupBy("district").agg(sum(col("aadhaar_generated") + col("rejected")).as("total_num_aadhaar_Enrollment_by_state"))
    val top_3_district_enroll_most_aadhaar = num_of_aadhar_enrollment_in_each_district.withColumn("rank", dense_rank().over(w_district)).filter(col("rank") <= 3)
    top_3_district_enroll_most_aadhaar.show(false)

    num_of_aadhar_generated_in_each_state.show(false)
  }

  def checkpoint4(spark: SparkSession, AadhaarDF: DataFrame) = {

    println("\n Initialising checkpoint4 \n")
    AadhaarDF.select("pincode").distinct.show(false)
    println("Aadhaar registrations rejected in Uttar Pradesh and Maharashtra = " + AadhaarDF.where(col("state") === "Uttar Pradesh" or col("state") === "Maharashtra").agg(sum("rejected")).first().get(0))
    //AadhaarDF.where(col("state")==="Uttar Pradesh" or col("state")==="Maharashtra").agg(sum("rejected")).show(1,false)


  }

  def checkpoint5(spark: SparkSession, AadhaarDF: DataFrame) = {

    println("\n Initialising checkpoint5 \n")

    val AadhaarDF_male = AadhaarDF.where(col("gender") === "M")
    val AadhaarDF_female = AadhaarDF.where(col("gender") === "F")

    val w = Window.orderBy(desc("percentage_aadhaar_male_by_female"))
    val aadhaar_genrated_male = AadhaarDF_male.groupBy("state").agg(sum("aadhaar_generated").as("aadhaar_generated_male"))
    val aadhaar_genrated_female = AadhaarDF_female.groupBy("state").agg(sum("aadhaar_generated").as("aadhaar_generated_female"))
    val disDF = AadhaarDF.groupBy("state").agg(sum(col("aadhaar_generated")).as("total_count"))
    val generatedDF = aadhaar_genrated_male.join(aadhaar_genrated_female, Seq("state"), "inner")
      .withColumn("percentage_aadhaar_male_by_female", ((col("aadhaar_generated_male") / col("aadhaar_generated_female")) * 100) - 100)
    val final_generated_DF = generatedDF.withColumn("rank", dense_rank().over(w)).filter(col("rank") <= 3) //.show(false)
    final_generated_DF.show(false)

    final_generated_DF.select("state").collect().map(_.getString(0)).foreach(state => {
      println(s"state = $state")
      val win = Window.orderBy(desc("percentage_aadhaar_rejected_female_by_male"))
      val aadhaar_rejected_male = AadhaarDF_male.where(col("state") === state).groupBy("district").agg(sum("rejected").as("aadhaar_rejected_male"))
      val aadhaar_rejected_female = AadhaarDF_female.where(col("state") === state).groupBy("district").agg(sum("rejected").as("aadhaar_rejected_female"))
      val rejectionDF = aadhaar_rejected_male.join(aadhaar_rejected_female, Seq("district"), "inner")
        .withColumn("percentage_aadhaar_rejected_female_by_male", ((col("aadhaar_rejected_female") / col("aadhaar_rejected_male")) * 100) - 100)
      rejectionDF.withColumn("rank", dense_rank().over(win)).filter(col("rank") <= 3).show(false)
    })

    val interval = 12;
    AadhaarDF.withColumn("range", col("age") - (col("age") % interval))
      .withColumn("range", concat(col("range"), lit(" - "), col("range") + interval))
      .groupBy(col("range"))
      .agg((((sum(col("aadhaar_generated")) / sum(col("rejected"))) * 100) - 100).as("acceptance_per"))
      .where(col("acceptance_per").isNotNull).show(false)
  }

  def Good_Bad_DF(spark: SparkSession) = {
    val all_DF = spark.read.option("inferschema", true).option("header", true).csv("""gs://dataproc-1e27335a-b915-40d8-b2c4-8b0cb9b83e41-asia-northeast2/Rajat/annual-enterprise-survey-2017-financial-year-provisional.csv""") //.count()
    val sch = all_DF.first.schema
    //println(sch)

    val good_DF = spark.read.schema(sch).option("header", true).option("mode", "DROPMALFORMED").csv("""gs://dataproc-1e27335a-b915-40d8-b2c4-8b0cb9b83e41-asia-northeast2/Rajat/annual-enterprise-survey-2017-financial-year-provisional.csv""")

    //All_DF = 23177 ; good_DF = 23172 ; bad_DF = 5
    val bad_DF = all_DF.except(good_DF)
    println(s"all_DF = ${all_DF.count} ; good_DF = ${good_DF.count} ; bad_DF =${bad_DF.count} ")
    bad_DF.show(false)
    val try_DF = spark.read.schema(sch).option("header", true).option("quote", "\"").option("escape", "\"").option("mode", "DROPMALFORMED").csv("""gs://dataproc-1e27335a-b915-40d8-b2c4-8b0cb9b83e41-asia-northeast2/Rajat/annual-enterprise-survey-2017-financial-year-provisional.csv""")
    val try_bad_DF = all_DF.except(try_DF)
    try_bad_DF.show(false)

  }


}

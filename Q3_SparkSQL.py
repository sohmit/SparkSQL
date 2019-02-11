#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 18:22:37 2018

@author: sohinimitra

Q3 Using SparkSQL
"""



from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *

if __name__ == "__main__":
    sc.stop()
 
    config = SparkConf().setAppName("Average Ratings").setMaster("local[1]")
    sc = SparkContext(conf = config)
    spark = SparkSession(sc)

    business = sc.textFile("business.csv")
    businessDF = business.map(lambda x : x.split("::")).toDF()
    businessDF = businessDF.select(col("_1").alias("BusinessID"), col("_2").alias("Address"), col("_3").alias("Categories"))
    businessDF = businessDF.filter(col('Address').contains('Stanford'))
    review = sc.textFile("review.csv")
    reviewDF = review.map(lambda x : x.split("::")).toDF()
    reviewDF = reviewDF.select(col("_1").alias("ReviewID"), col("_2").alias("UserID"), col("_3").alias("BusinessID"),col("_4").alias("Rating").cast("double"))
    joinDF = reviewDF.join(businessDF, reviewDF.BusinessID == businessDF.BusinessID, 'inner')
    joinDF = joinDF.select("UserID","Rating")
  
    joinDF.show()
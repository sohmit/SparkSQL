#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 16:23:06 2018

@author: sohinimitra

Q4 Using SparkSQL
"""
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *

if __name__ == "__main__":
    
 
    config = SparkConf().setAppName("Average Ratings").setMaster("local[1]")
    sc = SparkContext(conf = config)
    spark = SparkSession(sc)

    business = sc.textFile("business.csv")
    businessDF = business.map(lambda x : x.split("::")).toDF()
    businessDF = businessDF.select(col("_1").alias("BusinessID"), col("_2").alias("Address"), col("_3").alias("Categories"))
    
    review = sc.textFile("review.csv")
    reviewDF = review.map(lambda x : x.split("::")).toDF()
    reviewDF = reviewDF.select(col("_1").alias("ReviewID"), col("_2").alias("UserID"), col("_3").alias("BusinessID"),col("_4").alias("Rating").cast("double"))
    reviewDF = reviewDF.select("BusinessID","Rating").groupby("BusinessID").avg("Rating").orderBy("avg(Rating)", ascending = False).limit(10)
    joinDF = reviewDF.join(businessDF, reviewDF.BusinessID == businessDF.BusinessID, 'inner')
   
    joinDF = joinDF.distinct()
    joinDF.show()
 
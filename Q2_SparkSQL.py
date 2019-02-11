#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 19 13:11:01 2018

@author: sohinimitra
Q2 using SparkSQL
"""


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *

def createMutualFriendsList(line):
    
  user = line[0].strip()
  friends = line[1]
  friendList = []
  if(user != ''):
      for f in friends:
          f = f.strip()
          if(f != ''):
              if(int(user)<int(f)):
                  listValue = user + "," + f , set(friends)
              if(int(f)<int(user)):
                  listValue = f + "," + user , set(friends)
              friendList.append(listValue)
  return(friendList)


def countFriends(line):
    return(len(line[1]),line[0])
    
def getNameAndAddress(line):
    return (line[0],line[1],line[2],line[3])

def getUsers(line):
    users = line[1].split(",")
    return (users[0], (users[1],line[0]))

def splitUserData(line):
    line = line.split(",")
    return(line[0],(line[1],line[2],line[3]))
    
def formatUserData(line):
    return( line[1][0][0],((line[0],line[1][0][1]),line[1][1]) ) 
    
def splitUsers(line):
    return (line[0],line[1][0],line[1][1])

def combineUser(line):
    address = line[3]+line[4]+line[5]+line[6]+line[7]
    return (line[0],line[1],line[2],address)

    
def getOutput(line):
    
    countOfFriends = str(line[1][0][0][1])
    user1Info = line[1][0][1]
    user2Info = line[1][1]
    user1Fname = user1Info[0]
    user1Lname= user1Info[1]
    user1Address = user1Info[2]
    user2Fname = user2Info[0]
    user2Lname = user2Info[1]
    user2Address = user2Info[2]
    return (countOfFriends,user1Fname,user1Lname,user1Address,user2Fname,user2Lname,user2Address)
    #return("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}".format(countOfFriends,user1Fname,user1Lname,user1Address,user2Fname,user2Lname,user2Address))
    

if __name__ == "__main__":
    sc.stop()
    config = SparkConf().setAppName("MutualFriends - User Info").setMaster("local[2]")
    sc = SparkContext(conf = config)
    spark = SparkSession(sc)
    lines = sc.textFile("soc-LiveJournal1Adj.txt")
    lines = lines.map(lambda x : x.split("\t"))
    lines = lines.filter(lambda x : len(x) == 2)
    lines = lines.map(lambda x : [x[0],x[1].split(",")])
    lines = lines.flatMap(createMutualFriendsList)
    lines = lines.reduceByKey(lambda x,y: x.intersection(y))
    lines = lines.map(countFriends)
    lines = lines.sortByKey(ascending = False)
   
    lines = lines.map(getUsers)
    lines = lines.take(10)
    lines = sc.parallelize(lines)
    lines = lines.map(splitUsers)
    linesDF = lines.toDF()
    linesDF = linesDF.select(col("_1").alias("User_1"),col("_2").alias("User_2"),col("_3").alias("Count_of_friends")).limit(10)
  
    userdata = sc.textFile("userdata.txt")
    userdata = userdata.map((lambda x : x.split(",")))
    userdata = userdata.map(combineUser)
    userdataDF = userdata.toDF()
    userdataDF = userdataDF.select(col("_1").alias("UserID"),col("_2").alias("FirstName"),col("_3").alias("LastName"),col("_4").alias("Address"))
    
    joinDF = userdataDF.join(linesDF, linesDF.User_1 == userdataDF.UserID, 'inner')
   
    joinDF = joinDF.withColumnRenamed( "UserID", "UserID1")
    
    join2DF = userdataDF.join(joinDF, userdataDF.UserID == joinDF.User_2, 'inner')
    
    output = join2DF.drop("UserID","UserID1")
    output.show()
 
    sc.stop()
    
 

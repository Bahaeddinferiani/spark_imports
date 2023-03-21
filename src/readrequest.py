def readreq(spark,user,password,ip,port,database):
   dataframe = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://"+ip+":"+port+"/information_schema") \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable","(select * from tables where TABLE_SCHEMA = '"+database+"') as tables") \
    .load()
   array = dataframe.collect()
   newarray =[]
   for i in array:
    newarray.append(i.TABLE_NAME)
   return newarray
def readdatabases(spark,user,password,ip,port):
   dataframe = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://"+ip+":"+port+"/information_schema") \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable","tables") \
    .load()
   array = dataframe.collect()
   newarray =[]
   for i in array:
    newarray.append(i.TABLE_SCHEMA)
   newarray = list(dict.fromkeys(newarray))
   return newarray
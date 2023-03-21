def tableload(spark,user,password,ip,port,database,table):
   dataframe = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://"+ip+":"+port+"/"+database) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable",table) \
    .load()
   
   return dataframe
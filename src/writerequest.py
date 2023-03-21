from datetime import datetime

def filename():
    # time of request
    now = datetime.now().strftime("%d-%m-%Y/%H-%M-%S")
    # creating the file with current time
    
    return filename

def writereq(df,table):
    #url for hdfs
    # saving the data as csv file
    df.write.mode("overwrite").format('csv').save('hdfs://localhost:9000/'+table)
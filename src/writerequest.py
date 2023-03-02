from datetime import datetime

def filename():
    # time of request
    now = datetime.now().strftime("%d-%m-%Y/%H-%M-%S")
    # creating the file with current time
    filename = str(now)
    return filename

def writereq(df):
    #url for hdfs
    # saving the data as csv file
    df.write.format('csv').save('hdfs://localhost:9000/'+filename()+'/')
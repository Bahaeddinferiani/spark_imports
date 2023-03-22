from flask import *
from src.sparkbuilder import *
from src.readdatabases import *
from src.readtables import *
from src.writerequest import *
from src.tableload import *


from datetime import datetime
# For the counter 
def strfdelta(tdelta, fmt):
    d = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


#for static files (js,css...)
app = Flask(__name__, static_url_path='/static')

app.config['SESSION_TYPE'] = 'filesystem'
app.secret_key='smth secret'


def loadtable(spark,user,password,ip,port,schema,table):
     loadResult = tableload(spark,user,password,ip,port,schema,table)
     return loadResult



# spark settings for import 
def connection(user,password,ip,port):
   try:
         sparksession = sparkbuilder()
         databases = readdatabases(sparksession,user,password,ip,port)
         credentials = {"user":user,"password":password,"ip":ip,"port":port,"databases":databases}
         session['credentials'] = credentials
         return redirect('/databases')
   except Exception as e:
         return render_template("error.html",the_error=str(e))



@app.route('/databases', methods =["GET", "POST"])
def databaseselection():
      credentials = session.get('credentials')
      databases = credentials.get("databases")      
      if request.method == "POST":
            database = request.form.get("database")
            session['database'] = database
            sparksession = sparkbuilder()
            user = credentials.get("user")
            ip = credentials.get("ip")
            port = credentials.get("port")
            password = credentials.get("password")
            tables = readtables(sparksession,user,password,ip,port,database)
            session['tables'] = tables
            return redirect('/tables')
      credentials = session.get('credentials')
      databases = credentials.get('databases')
      return render_template("database.html", len=len(databases),databases = databases)



@app.route('/tables', methods =["GET", "POST"])
def tablesselection():
      tables = session.get('tables')
      if request.method == "POST":
            credentials = session.get('credentials')
            table = request.form.get("table")
            user = credentials.get("user")
            ip = credentials.get("ip")
            port = credentials.get("port")
            password = credentials.get("password")
            t1 = datetime.strptime(datetime.now().strftime("%H:%M:%S"), "%H:%M:%S")
            spark = sparkbuilder()
            database = session['database']
            result = tableload(spark,user,password,ip,port,database,table)
            writereq(result,table)
            t2 = datetime.strptime(datetime.now().strftime("%H:%M:%S"), "%H:%M:%S")
            delta = t2 - t1
            return render_template("success.html",name=table,time=strfdelta(delta,"{seconds}"))
      tables = session.get('tables')
      return render_template("table.html", len=len(tables),tables = tables)



# credentials form and import
@app.route('/', methods =["GET", "POST"])
def spark():
      if request.method == "POST":
            # inputs from the form
            username = request.form.get("username")
            password = request.form.get("password")
            ip = request.form.get("ip")
            port = request.form.get("port")
            return connection(username,password,ip,port)
      return render_template("import.html") 
if __name__=='__main__':
   app.run()



from flask import *
from src.sparkbuilder import *
from src.readrequest import *
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
def connection(user,password,ip,port,schema):
   try:
         sparksession = sparkbuilder()
         readResult = readreq(sparksession,user,password,ip,port,schema)

         if (len(readResult) == 0):
              return render_template("error.html",the_error="Database \""+schema+"\" has no tables !")
         credentials = {"schema":schema,"user":user,"password":password,"ip":ip,"port":port,"tables":readResult}
         session['credentials'] = credentials
         return redirect('/table-selection')
   except Exception as e:
         return render_template("error.html",the_error=str(e))

@app.route('/table-selection', methods =["GET", "POST"])
def tablesSelection():
      credentials = session.get('credentials')
      readResult = credentials.get("tables")      
      if request.method == "POST":
            table = request.form.get("table")
            user = credentials.get("user")
            ip = credentials.get("ip")
            port = credentials.get("port")
            password = credentials.get("password")
            t1 = datetime.strptime(datetime.now().strftime("%H:%M:%S"), "%H:%M:%S")
            spark = sparkbuilder()
            schema = credentials.get("schema")
            result = tableload(spark,user,password,ip,port,schema,table)
            writereq(result,table)
            t2 = datetime.strptime(datetime.now().strftime("%H:%M:%S"), "%H:%M:%S")
            delta = t2 - t1
            return render_template("success.html",name=table,time=strfdelta(delta,"{seconds}"))



      credentials = session.get('credentials')
      readResult = credentials.get("tables")      
      return render_template("table.html", len=len(readResult),readResult = readResult)


# credentials form and import
@app.route('/', methods =["GET", "POST"])
def spark():
      if request.method == "POST":
            # inputs from the form
            username = request.form.get("username")
            password = request.form.get("password")
            ip = request.form.get("ip")
            port = request.form.get("port")
            schema = request.form.get("schema")
            return connection(username,password,ip,port,schema)
      return render_template("import.html") 
if __name__=='__main__':
   app.run()



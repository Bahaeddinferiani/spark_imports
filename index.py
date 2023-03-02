from flask import Flask, request, render_template
from src.sparkbuilder import *
from src.readrequest import *
from src.writerequest import *
from datetime import datetime

def strfdelta(tdelta, fmt):
    d = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)
#for static files (js,css...)
app = Flask(__name__, static_url_path='/static')

# spark settings for import 
def connection(user,password):
   try:
         # building spark context
         t1 = datetime.strptime(datetime.now().strftime("%H:%M:%S"), "%H:%M:%S")

         spark = sparkbuilder()         
         # read api call
         df = readreq(spark,user,password)
         # write api call
         writereq(df)
         t2 = datetime.strptime(datetime.now().strftime("%H:%M:%S"), "%H:%M:%S")
         delta = t2 - t1
         return render_template("success.html",name=filename(),time=strfdelta(delta,"{seconds}"))
   
   except Exception as e:
         return render_template("error.html",the_error=str(e))


# home controller
@app.route('/', methods =["GET", "POST"])
def home():
      return render_template("home.html")



# credentials form and import
@app.route('/import', methods =["GET", "POST"])
def spark():
    if request.method == "POST":
       # inputs from the form
         username = request.form.get("username")
         password = request.form.get("password")
         return connection(username,password)

    return render_template("form.html")
 
if __name__=='__main__':
   app.run()



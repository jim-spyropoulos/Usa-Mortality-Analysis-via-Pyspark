from flask import Flask,request, render_template, Blueprint
from spark_engine import SparkEngine
main = Blueprint('main', __name__)

#includes for spark_logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@main.route('/')
def index():
   return render_template("index.html")

@main.route('/second_screen',methods = ['POST']) 
def second_screen():
	if request.method == 'POST':
		result = request.form['q']
		return render_template("second_screen.html",choice = result)

@main.route('/life_expectance',methods = ['POST'])
def life_expectance():
	if request.method == 'POST':

		from_year = int(request.form['from_year'])
		to_year = int(request.form['to_year'])
		res = spark_engine.prosdokimo_zois(from_year,to_year) 
		labels = []
		values = []
		for key in res: 
			labels.append(key)
			values.append(res[key])
			
			
		return render_template("life_expectance.html",values=values, labels=labels)
		'''take the info from the form 
		execute tha spark_engine functionality
		then return a render_template with results using javascript for representation '''

@main.route('/GunsVSvehicles',methods = ['POST'])
def GunsVSVehicles():
	if request.method == 'POST': 

		from_year = int(request.form['from_year'])
		to_year = int(request.form['to_year'])
		(resF, resV) = spark_engine.GunsVSVehicles(from_year,to_year)

		labels = []
		valuesF = []
                valuesV = []
		for key in resF:
			labels.append(key)
			valuesF.append(resF[key])

		for key in resV:
			valuesV.append(resV[key])

		return render_template("guns_vs_vehicles.html", valuesF=valuesF, valuesV=valuesV, labels=labels)

@main.route('/death_causes', methods = ['POST'])
def death_causes():
	year = int(request.form['year'])
	causes = int(request.form['causes'])
	res = spark_engine.deathcauses(year,causes)

	return render_template("death_causes.html",causes_no = causes, year = year,causes = res)

@main.route('/race_causes',methods = ['POST'])
def race_causes():
	if request.method == 'POST':
		user_result = request.form['r']
		from_year = int(request.form['from_year'])
		to_year = int(request.form['to_year'])
		logger.info(user_result)
		results = spark_engine.race_causes(user_result,from_year,to_year)
		logger.info(results)

                labels = []
		values = []
		for key in results:
			labels.append(key[0])
			values.append(key[1])

		return render_template("race_causes.html", values=values, labels=labels)


def create_app(sc):
	global spark_engine

	spark_engine = SparkEngine(sc)
	app = Flask(__name__)
	app.register_blueprint(main)
	return app 


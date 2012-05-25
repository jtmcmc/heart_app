import veritable
import os
from flask import Flask,jsonify,request
from flask import render_template
import simplejson as sj

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict')
def predict():
	API_KEY = '<API_KEY>'
	api = veritable.connect(api_key=API_KEY)
	table = api.get_table('heart_data')
	analysis = table.get_analysis('heart_analysis')
	foo = request.args.to_dict().keys()
	new_patient = sj.loads(foo[0])
	del new_patient['']
	for k in new_patient:
		new_patient[k] = float(new_patient[k])
	new_patient['num'] = None
	prediction = analysis.predict(new_patient)
	return jsonify(predicted=prediction['num'])


if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

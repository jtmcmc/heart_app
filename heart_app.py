import veritable
from veritable.utils import read_csv, clean_data
import sys
import re

""" Attribute Information:
   -- Only 14 used
      -- 1. #3  (age)       
      -- 2. #4  (sex)       
      -- 3. #9  (cp)        
      -- 4. #10 (trestbps)  
      -- 5. #12 (chol)      
      -- 6. #16 (fbs)       
      -- 7. #19 (restecg)   
      -- 8. #32 (thalach)   
      -- 9. #38 (exang)     
      -- 10. #40 (oldpeak)   
      -- 11. #41 (slope)     
      -- 12. #44 (ca)        
      -- 13. #51 (thal)      
      -- 14. #58 (num)       (the predicted attribute)
"""


def make_veritable_model(api_key,schema,data):
	api = veritable.connect(api_key=api_key)
	print(list(api.get_tables()))
	table = api.get_table('heart_data')
	analysis = table.get_analysis('heart_analysis')
	print analysis.state

	new_patient = {'age' : 48,
			  'sex'	     : 0,
			  'cp'       : 2,
			  'trestbps' : 110,
			  'chol'     : 200,
			  'fbs'      : 0.0,
			  'restecg'  : 1.0,
			  'thalach'  : 0.0,
			  'exang'    : 100,
			  'oldpeak'  : 0,
			  'slope'    : .2,
			  'ca'       : 0,
			  'thal'     : 0,
			  'num'      : None,
			  }
	prediction = analysis.predict(new_patient)
	print prediction['num']
	print prediction.uncertainty
	print prediction.distribution

"""	rows = read_csv(data)
	clean_data(rows,schema)
	table = api.create_table(table_id='heart_data')
	table.batch_upload_rows(rows)
	analysis = table.create_analysis(schema=schema, 
                                 analysis_id='heart_analysis')
	analyses = list(table.get_analyses())
	print len(analyses), 'analyses found for', table
	for a in analyses:
		print a, a.state

	analysis.wait()
	print a, a.state
"""

if __name__ == '__main__':
	fi = open('processed.cleveland.data.txt')
	schema = {'age'      : {'type' : 'real'},
			  'sex'	     : {'type' : 'real'},
			  'cp'       : {'type' : 'real'},
			  'trestbps' : {'type' : 'real'},
			  'chol'     : {'type' : 'real'},
			  'fbs'      : {'type' : 'real'},
			  'restecg'  : {'type' : 'real'},
			  'thalach'  : {'type' : 'real'},
			  'exang'    : {'type' : 'real'},
			  'oldpeak'  : {'type' : 'real'},
			  'slope'    : {'type' : 'real'},
			  'ca'       : {'type' : 'real'},
			  'thal'     : {'type' : 'real'},
			  'num'      : {'type' : 'count'},
			  }
	raw_data = fi.readlines()
	API_KEY = '<API_KEY>'
	#pre_process 
#	process_data = []
#	for entry in raw_data:
#		if not re.search('\?',entry):
#			print entry
#			entry = [float(x.strip()) for x in entry.split(',')]
#			if len(entry) == 14:
#				process_data.append(entry)
#
#	print len(process_data)
#	print process_data
	make_veritable_model(API_KEY,schema,'processed.cleveland.data.txt')
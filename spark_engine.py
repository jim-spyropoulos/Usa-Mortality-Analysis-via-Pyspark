import os,sys
import re
#import matplotlib.pyplot as plt
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession
import operator

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def make_age_dict(text):
	   	ages = {}
	   	for line in text:
	   		k = ""
	   		v=""
	   		flag = 0
	   		#first find the key
	   		for char in line:
	   			if (flag == 0):
	   				if (char!=':'):
	   					k = k + char
	   					#print k
	   				else:
	   					flag = 1 
				else:
					if (v!='\n'):
						v = v + char
			#then find the value
			#print line
			#print k,v
			ages[k] = v[:-1]
		ages_mean= {}
		for key in ages:
			#print ages[key]
			#if age > 85 years consider as class representative the 85 years
			if (ages[key] == '85 years and over'):
				ages_mean[key] = 85
			#if age not stated consider as class representative	0 years
			elif (ages[key] == 'Age not stated'):
				ages_mean[key] = 0
			#if age is under 1 year consider as class representative 1 year
			elif (ages[key] == 'Under 1 year (includes not stated infant ages)'):
				ages_mean[key] = 1
			#else consider the mean value of higher and lower bound of each age class
			else:
				num1 = 0
				num2 = 0
				flag1 = 0 
				a = ''
				b = ''
				for char in ages[key]:
					if (flag1==0):
						if (char!=' '):
							a = a + char
						else:
							num1 = int(a)
							flag1 = 1
							#print a
					else:
						if (char=='-' or char==' '):
							continue
						elif (char=='y'):
							num2 = int(b)
						else:
							b = b + char
						

				ages_mean[key] = float(num1+num2)/2
				#print num1,num2
		return ages_mean
def make_death_causes_dict(text):
    death_causes = {}
    for line in text:
        k = ""
        v=""
        flag = 0
        #first find the key
        for char in line:
            if (flag == 0):
                if (char!=':'):
                    k = k + char
                    #print k
                else:
                    flag = 1
            else:
                if (v!='\n'):
                    v = v + char
 
        #then find the value
        #print line
        #print k,v
        death_causes[k] = v[:-1]
    return death_causes


def race_cause_codes(text):
	a = {}
	for line in text:
		k = ""
		v=""
		flag = 0
		#first find the key
		for char in line:
			if (flag == 0):
				if (char!=':'):
					k = k + char
					#print k
				else:
					flag = 1
			else:
				if (v!='\n'):
					v = v + char

		#then find the value
		#print line
		#print k,v
		a[k] = v[:-1]
	return a


class SparkEngine:


	def prosdokimo_zois(self,from_year,to_year):
		results = {}
		for year in range(from_year,to_year+1):
			#create Temporary view of the Dataframe
			self.df_list[year-2005].createOrReplaceTempView("deaths")
			#create buckets with number of deads per age_class except age_class 12
			deathage = self.sc.sql('SELECT COUNT(*) as deads,age_recode_12 as age FROM deaths WHERE age_recode_12!="12" GROUP BY age_recode_12 ')
			#count all the deaths of the year except those with death code 12
			alldeaths = self.sc.sql('SELECT COUNT(*) as alldeaths from deaths WHERE age_recode_12!="12"')
			for row in alldeaths.collect():
				deaths = row.alldeaths
				break
			sum1 = 0.0
			
			for row in deathage.collect():
				sum1 = sum1 + float(row.deads)*self.ages_mean[row.age]
			prosdokimo = sum1 / deaths
			results[year] = prosdokimo
		#return the result dict, as {year:prosdokimo_zois}
		return results

	def GunsVSVehicles(self,from_year,to_year):

		sum_deaths_by_firearms = 0
		sum_deaths_by_vehicle = 0

		f_deaths = {}
		v_deaths = {}
		for year in range(from_year,to_year+1):

		    self.df_list[year-2005].createOrReplaceTempView("deaths")
		    #perform a count of all deaths caused by guns
		    firearms_deaths = self.sc.sql("SELECT * FROM deaths WHERE 113_cause_recode IN (119,132,125,128)").count()
		    #store them in f_deaths dictionary
		    f_deaths[year] = firearms_deaths 
		    sum_deaths_by_firearms = sum_deaths_by_firearms + firearms_deaths

		    #perform a count of all deaths caused by vehicles
		    vehicle_deaths = self.sc.sql("SELECT * FROM deaths WHERE 113_cause_recode == 114").count()
		    #store them in v_deaths dictionary
		    v_deaths[year] = vehicle_deaths
		    sum_deaths_by_vehicle = sum_deaths_by_vehicle + vehicle_deaths

		logger.info(str(f_deaths))
		return f_deaths, v_deaths

	def deathcauses(self,year,causes):
		results = []
		self.df_list[year-2005].createOrReplaceTempView("deaths")
		#select top_causes causes from the year given as arguments
		deathsByReason = self.sc.sql("SELECT COUNT(*) as deads, 358_cause_recode as cause FROM deaths GROUP BY 358_cause_recode ORDER BY COUNT(*) DESC LIMIT "+str(causes))
		for row in deathsByReason.collect():
		    results.append((self.death_causes[row.cause],row.deads))
		 
		#return a list of causes for the year    
		return results

	def race_causes(self, choice, from_year, to_year):
		race_case = choice

		total_deads = 0

		b = {} 

		for year in range(from_year,to_year+1):
			#for each year given in arguments select the death causes for the given race
		    self.df_list[year-2005].createOrReplaceTempView("deaths")

		    people = self.sc.sql("SELECT * FROM deaths WHERE race == " + race_case)
		    thisyeardeads = people.count()
		    total_deads = total_deads + thisyeardeads

		    people.createOrReplaceTempView("people")
		    deathsByReason = self.sc.sql("SELECT COUNT(*) as deads, 39_cause_recode as cause FROM people GROUP BY 39_cause_recode ORDER BY COUNT(*) DESC")
		    deathsByReason.createOrReplaceTempView("newdeaths")

		    for row in deathsByReason.collect():
			if b.has_key(str(row.cause)):
			    b[str(row.cause)]= b[str(row.cause)]+row.deads
			else:
			    b[str(row.cause)] = row.deads

		print b

		#sorting
		sorted_b = sorted(b.items(), key=operator.itemgetter(1), reverse=True)
		print(sorted_b)
		first_five=[]
          	print '###############################################'
	  	logger.info(self.race_causes)
		for value in sorted_b[:5]:
    		    lst=list(value)
    		    lst[0]=self.race_causes_short_names[value[0]]
    		    first_five.append(lst)


		return first_five


	def __init__(self, sc):

   	  logger.info("Starting up the Spark Engine: ")
   	  self.sc = sc
   	  self.years = []
   	  self.df_list = []
   	  #Set the dictionaries
   	  
      #read age_recodes to class dict ages_mean
      text = open('./agerecode12','r')
      self.ages_mean = make_age_dict(text)
      text.close()
      #read death causes to class dict death_causes
      text = open('./death_causes','r')
      self.death_causes = make_death_causes_dict(text)
      text.close()
      #read age_recodes to class dict race_causes_full_names
      text = open('./race_cause_codes','r')
      self.race_causes_full_names = race_cause_codes(text)
      text.close()
      #read age_recodes to class dict race_causes
      text = open('./race_cause_codes2','r')
      self.race_causes_short_names = race_cause_codes(text)
      text.close()


   	  for year in range(2005, 2016):
   	  	logger.info("Reading and adding DataFrame for year "+str(year))
   	  	df = self.sc.read.option("header", True).csv("<path_to_dataset_file/"+str(year)+"_data.csv").cache()
   	  	self.df_list.append(df)
   	  	self.years.append(year)



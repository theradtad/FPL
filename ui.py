import json,csv
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors,DenseVector
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
import datetime

#all paths for required files
players_json_path = 'hdfs://localhost:9000/input_proj/players2.json'
players_chemistry_path = 'hdfs://localhost:9000/input_proj/players_chemistry2.json'
players_ratings_path = 'hdfs://localhost:9000/input_proj/player_ratings.json'
matches_json_path = '/home/chirag/Desktop/BigDataProject//matches_details.json'
input_player_data_path = '/home/chirag/Desktop/BigDataProject/inp_player.json'
output_player_data_path = '/home/chirag/Desktop/BigDataProject/out_player.json'
input_match_data_path = '/home/chirag/Desktop/BigDataProject/inp_match.json'
output_match_data_path = '/home/chirag/Desktop/BigDataProject/out_match.json'
input_prediction_data_path = '/home/chirag/Desktop/BigDataProject/inp_predict.json'
output_prediction_data_path = '/home/chirag/Desktop/BigDataProject/out_predict.json'

spark = SparkSession.builder.appName("User_Interface").getOrCreate()
sqlcontext=SQLContext(spark)

#writes the required player profile to a JSON file
def displayPlayerProfile(input_data):
	players_data_df = sqlcontext.read.json(players_json_path)
	players_data_df = players_data_df.filter(players_data_df.name == input_data["name"])
	if(players_data_df.count() == 0):
		d={
			"Error":"Invalid"
		}
		with open(output_player_data_path, "w") as jsonFile:
			jsonFile.write(json.dumps(d,indent=4))

	player = players_data_df.collect()[0]
	output_dict=dict()
	output_dict={
		"Id": player.Id,
		"birthArea": player.birthArea,
		"birthDate": player.birthDate,
		"foot":player.foot,
		"height":player.height,
		"name":player.name,
		"number_of_fouls":player.number_of_fouls,
		"number_of_goals":player.number_of_goals,
		"number_of_matches":player.number_of_matches,
		"number_of_own_goals":player.number_of_own_goals,
		"number_of_matches":player.number_of_matches,
		"number_of_own_goals":player.number_of_own_goals,
		"pass_accuracy":player.pass_accuracy,
		"passportArea":player.passportArea,
		"role":player.role,
		"number_of_shots_on_target":player.number_of_shots_on_target,
		"weight":player.weight,
	}
	with open(output_player_data_path, "w") as jsonFile:
		jsonFile.write(json.dumps(output_dict,indent=4))

#writes the required match details to a JSON file
def displayMatchDetails(input_data):
	with open(matches_json_path, "r") as jsonFile:
		matches_data = json.load(jsonFile)
	match_label = input_data["label"]
	for match_id in matches_data:
		if(matches_data[match_id]["label"]==match_label):
			with open(output_match_data_path, "w") as jsonFile:
				jsonFile.write(json.dumps(matches_data[match_id],indent=4))
			break
	d = {
		"Error":"Invalid"
	}
	with open(output_match_data_path, "w") as jsonFile:
		jsonFile.write(json.dumps(d,indent=4))


def clustering_players(df_players):
    l = []
    for i in df_players.rdd.collect():
        b=(i.Id,i.number_of_matches,i.birthDate,DenseVector([float(i.pass_accuracy),float(i.number_of_goals),float(i.number_of_own_goals),float(i.number_of_fouls),float(i.number_of_shots_on_target)]))
        l.append(b)

    df = spark.sparkContext.parallelize(l).toDF(["Id","number_of_matches","birthDate","features"])
    dataset = df.filter(df.number_of_matches > 0)
    dataset = df.select("features")

    # Trains a k-means model.
    kmeans = KMeans().setK(5).setSeed(1)
    model = kmeans.fit(dataset)

    # Make predictions
    predicted_clusters = model.transform(df)
    return predicted_clusters

def get_chemistry_cluster_stats(joined_records_chemistry):
	cluster0 = joined_records_chemistry.filter(joined_records_chemistry.prediction == 0).select("chemistry")
	cluster1 = joined_records_chemistry.filter(joined_records_chemistry.prediction == 1).select("chemistry")
	cluster2 = joined_records_chemistry.filter(joined_records_chemistry.prediction == 2).select("chemistry")
	cluster3 = joined_records_chemistry.filter(joined_records_chemistry.prediction == 3).select("chemistry")
	cluster4 = joined_records_chemistry.filter(joined_records_chemistry.prediction == 4).select("chemistry")


	sum_chemistry0 = cluster0.rdd.map(lambda x: (1,x[0])).reduceByKey(lambda x,y: x + y).collect()[0][1]
	sum_chemistry1 = cluster1.rdd.map(lambda x: (1,x[0])).reduceByKey(lambda x,y: x + y).collect()[0][1]
	sum_chemistry2 = cluster2.rdd.map(lambda x: (1,x[0])).reduceByKey(lambda x,y: x + y).collect()[0][1]
	sum_chemistry3 = cluster3.rdd.map(lambda x: (1,x[0])).reduceByKey(lambda x,y: x + y).collect()[0][1]
	sum_chemistry4 = cluster4.rdd.map(lambda x: (1,x[0])).reduceByKey(lambda x,y: x + y).collect()[0][1]

	average_chemistry = dict()

	average_chemistry[0] = (sum_chemistry0/cluster0.count())
	average_chemistry[1] = (sum_chemistry1/cluster1.count())
	average_chemistry[2] = (sum_chemistry2/cluster2.count())
	average_chemistry[3] = (sum_chemistry3/cluster3.count())
	average_chemistry[4] = (sum_chemistry4/cluster4.count())

	return average_chemistry
    
def regression_on_cluster(joined_records_ratings,cluster_number,player_birth_date,match_date):

	joined_records_ratings = joined_records_ratings.filter((joined_records_ratings.number_of_matches < 5) & (joined_records_ratings.prediction == cluster_number)).select(["Id","playerid_date","rating","birthDate"])
	joined_records_ratings = joined_records_ratings.filter(joined_records_ratings.playerid_date.date != "0000-00-00")

	l1 = []
	for j in joined_records_ratings.rdd.collect():
		dateArraycur = j.playerid_date.date.split('-')
		date_current = datetime.datetime(int(dateArraycur[0]),int(dateArraycur[1]),int(dateArraycur[2]))
		dateArrybirth = j.birthDate.split('-')
		date_birth = datetime.datetime(int(dateArrybirth[0]),int(dateArrybirth[1]),int(dateArrybirth[2]))
		dt_age = date_current - date_birth
		age = (dt_age.days)
		squareAge = age*age
		b=(j.Id,DenseVector([float(age),float(squareAge)]),j.rating)
		l1.append(b)
	
	df2_normal_features = spark.sparkContext.parallelize(l1).toDF(["Id","features","label"])
	df_train_reg = df2_normal_features.select(["features","label"])

	#Fit the model
	lr = LinearRegression(featuresCol = 'features', labelCol='label', maxIter=10, regParam=0.0, elasticNetParam=0.0)
	lrModel = lr.fit(df_train_reg)

	l2 = []
	dateArraycur = match_date.split('-')
	date_current = datetime.datetime(int(dateArraycur[0]),int(dateArraycur[1]),int(dateArraycur[2]))
	dateArrybirth = player_birth_date.split('-')
	date_birth = datetime.datetime(int(dateArrybirth[0]),int(dateArrybirth[1]),int(dateArrybirth[2]))
	dt_age = date_current - date_birth
	age = (dt_age.days)
	squareAge = age*age
	b=(DenseVector([float(age),float(squareAge)]),1)
	l2.append(b)
	df_test = spark.sparkContext.parallelize(l2).toDF(["features","label"])
	df_test = df_test.select("features")

	#transform
	lr_predictions = lrModel.transform(df_test)
	predicted_rating = lr_predictions.collect()[0].prediction
	
	return predicted_rating

def regression_on_player(joined_records_ratings,player_id,birthDate,match_date):
	joined_records_ratings = joined_records_ratings.filter(joined_records_ratings.Id == player_id).select(["Id","playerid_date","rating","birthDate"])
	joined_records_ratings = joined_records_ratings.filter(joined_records_ratings.playerid_date.date != "0000-00-00")

	l1 = []
	dateArrybirth = birthDate.split('-')
	date_birth = datetime.datetime(int(dateArrybirth[0]),int(dateArrybirth[1]),int(dateArrybirth[2]))

	for j in joined_records_ratings.rdd.collect():
		dateArraycur = j.playerid_date.date.split('-')
		date_current = datetime.datetime(int(dateArraycur[0]),int(dateArraycur[1]),int(dateArraycur[2]))
		dt_age = date_current - date_birth
		age = (dt_age.days)
		squareAge = age*age
		b=(j.Id,DenseVector([float(age),float(squareAge)]),j.rating)
		l1.append(b)
	
	df2_normal_features = spark.sparkContext.parallelize(l1).toDF(["Id","features","label"])
	df_train_reg = df2_normal_features.select(["features","label"])

	#Fit the model
	lr = LinearRegression(featuresCol = 'features', labelCol='label', maxIter=10, regParam=0.0, elasticNetParam=0.0)
	lrModel = lr.fit(df_train_reg)

	df_train_reg.show()

	l2 = []
	dateArraycur = match_date.split('-')
	date_current = datetime.datetime(int(dateArraycur[0]),int(dateArraycur[1]),int(dateArraycur[2]))
	dt_age = date_current - date_birth
	age = (dt_age.days)
	squareAge = age*age
	b=(DenseVector([float(age),float(squareAge)]),1)
	l2.append(b)
	df_test = spark.sparkContext.parallelize(l2).toDF(["features","label"])
	df_test = df_test.select("features")


	#transform
	lr_predictions = lrModel.transform(df_test)
	predicted_rating = lr_predictions.collect()[0].prediction
	
	return predicted_rating


def match_prediction(input_data):
	df_players= sqlcontext.read.json(players_json_path)
	player_name_and_role_columns = ["name","role","Id"]
	player_name_and_role_df= df_players.select(player_name_and_role_columns)

	#stores the number of players in each role for a team
	team_1_details={
		"GK":0,
		"DF":0,
		"MD":0,
		"FW":0
	}
	team_2_details={
		"GK":0,
		"DF":0,
		"MD":0,
		"FW":0
	}

	team_1 = input_data["team1"]
	team_2= input_data["team2"]
	team_1_name = team_1.pop("name")
	team_2_name = team_2.pop("name")
	
	players_in_teams = []
	team1_players = []
	team2_players = []
    #updates the player roles in dictionary
	for player_no in team_1:
		player_name = team_1[player_no]
		row_of_player = player_name_and_role_df.filter(player_name_and_role_df.name == player_name)
		player_role = row_of_player.collect()[0].role
		player_id = row_of_player.collect()[0].Id
		players_in_teams.append(player_id)
		team1_players.append(player_id)
		team_1_details[player_role]+=1
		
	for player_no in team_2:
		player_name = team_2[player_no]
		row_of_player = player_name_and_role_df.filter(player_name_and_role_df.name == player_name)
		player_role = row_of_player.collect()[0].role
		player_id = row_of_player.collect()[0].Id
		players_in_teams.append(player_id)
		team2_players.append(player_id)
		team_2_details[player_role]+=1
		
    #check if lineups meet requirements
	invalid=0
	if(team_1_details["GK"]!=1 and team_2_details["GK"]!=1):
		invalid=1
	if(team_1_details["DF"]<2 and team_2_details["DF"]<2):
		invalid=1
	if(team_1_details["MD"]<2 and team_2_details["MD"]<2):
		invalid=1
	if(team_1_details["FW"]<1 and team_2_details["FW"<2]):
		invalid=1

    # if invalid, print and return
	if(invalid==1):
		final_dict = {"result":"Invalid"}
		with open(output_match_data_path) as f:
			f.write(json.dumps(final_dict,indent=4))
		return 0

    # #player chemistry read from
	df_players_chemistry= sqlcontext.read.json(players_chemistry_path)
	player_chem = dict()

    #clustering players and storing the resulting transformation in dataframe
	clustered_df = clustering_players(df_players)
	clustered_df = clustered_df.select(["Id","number_of_matches","prediction","birthDate"])

	#joined dataframe for chemistry and clustering
	joined_records_chemistry=clustered_df.join(df_players_chemistry,clustered_df.Id==df_players_chemistry.player1_player2.player_id_1)

	average_chemistry_dictionary = get_chemistry_cluster_stats(joined_records_chemistry)
	#chemistry coeffecient
	for player_1 in team1_players:
		row_of_player1 = df_players.filter(df_players.Id == player_1)
		num_matches1 = row_of_player1.collect()[0].number_of_matches
		sum_chem = 0
		cnt = 0
		for player_2 in team1_players:
			row_of_player2 = df_players.filter(player_name_and_role_df.Id == player_2)
			num_matches2 = row_of_player2.collect()[0].number_of_matches
			if(player_1 != player_2):
				if((num_matches1 >= 5 and num_matches2 >= 5) or (num_matches1 <= 5 and num_matches2 <= 5)):
					row_of_chem = df_players_chemistry.filter((df_players_chemistry.player1_player2.player_id_1  == player_1) & (df_players_chemistry.player1_player2.player_id_2  == player_2))
					sum_chem += row_of_chem.collect()[0].chemistry
					cnt+=1
				else:
					player_1_rows = joined_records_chemistry.filter(joined_records_chemistry.Id == player_1)
					player_2_rows = joined_records_chemistry.filter(joined_records_chemistry.Id == player_2)
					player_1_cluster = player_1_rows.collect()[0].prediction
					player_2_cluster = player_2_rows.collect()[0].prediction
					sum_chem += ((average_chemistry_dictionary[player_1_cluster]+average_chemistry_dictionary[player_1_cluster])/2)
					cnt+=1
		player_chem[player_1] = (sum_chem/cnt)
		
	for player_1 in team2_players:
		row_of_player1 = df_players.filter(df_players.Id == player_1)
		num_matches1 = row_of_player1.collect()[0].number_of_matches
		sum_chem = 0
		cnt = 0
		for player_2 in team2_players:
			row_of_player2 = df_players.filter(player_name_and_role_df.Id == player_2)
			num_matches2 = row_of_player2.collect()[0].number_of_matches
			if(player_1 != player_2):
				if((num_matches1 >= 5 and num_matches2 >= 5) or (num_matches1 <= 5 and num_matches2 <= 5)):
					row_of_chem = df_players_chemistry.filter((df_players_chemistry.player1_player2.player_id_1  == player_1) & (df_players_chemistry.player1_player2.player_id_2  == player_2))
					sum_chem += row_of_chem.collect()[0].chemistry
					cnt+=1
				else:
					player_1_rows = joined_records_chemistry.filter(joined_records_chemistry.Id == player_1)
					player_2_rows = joined_records_chemistry.filter(joined_records_chemistry.Id == player_2)
					player_1_cluster = player_1_rows.collect()[0].prediction
					player_2_cluster = player_2_rows.collect()[0].prediction
					sum_chem += ((average_chemistry_dictionary[player_1_cluster]+average_chemistry_dictionary[player_1_cluster])/2)
					cnt+=1
		player_chem[player_1] = (sum_chem/cnt)

	#input player ratings
	df_player_ratings = sqlcontext.read.json(players_ratings_path)
	joined_records_ratings = clustered_df.join(df_player_ratings,clustered_df.Id==df_player_ratings.playerid_date.player_id)
	player_ratings_dictionary = dict()

	#update player ratings
	for player in players_in_teams:
		row_of_player = df_players.filter(df_players.Id == player)
		num_matches = row_of_player.collect()[0].number_of_matches
		if(num_matches < 5):
			row_of_player = clustered_df.filter(clustered_df.Id == player)
			cluster_number = row_of_player.collect()[0].prediction
			birthdate =row_of_player.collect()[0].birthDate
			player_ratings_dictionary[player] = regression_on_cluster(joined_records_ratings,cluster_number,birthdate,input_data["date"])
		else:
			row_of_player = clustered_df.filter(clustered_df.Id == player)
			birthdate =row_of_player.collect()[0].birthDate
			player_ratings_dictionary[player] = regression_on_player(joined_records_ratings,player,birthdate,input_data["date"])

	sum_team_a = 0
	for player in team1_players:
		strength = player_chem[player] * player_ratings_dictionary[player]
		sum_team_a+=strength
	
	strength_team_a = (sum_team_a/11)

	sum_team_b = 0
	for player in team2_players:
		strength = player_chem[player] * player_ratings_dictionary[player]
	
	strength_team_b = (sum_team_b/11)

	chance_of_a_winning = (0.5 + strength_team_a - ((strength_team_a+strength_team_b)/2)) * 100
	chance_of_b_winning = 100 - chance_of_a_winning

	d = {
		"team1":{
			"name": team_1_name,
			"winning chance": chance_of_a_winning
		},
		"team2":{
			"name": team_2_name,
			"winning chance": chance_of_b_winning
		}
	}
	
	with open(output_prediction_data_path, "w") as jsonFile:
		jsonFile.write(json.dumps(d,indent=4))
	return

file_path = sys.argv[1]
with open(file_path, "r") as jsonFile:
	input_data = json.load(jsonFile)

if("req_type" in input_data.keys()):
	if(input_data["req_type"] == 1):
		det = match_prediction(input_data)
	elif(input_data["req_type"] == 2):
		displayPlayerProfile(input_data)
else:
	displayMatchDetails(input_data)
		

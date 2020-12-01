import json,csv
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans

#all paths for required files
players_json_path = 'hdfs://localhost:9000/input_proj/players2.json'
matches_json_path = '/home/chirag/Desktop/BigDataProject/matches_details.json'
input_player_data_path = '/home/chirag/Desktop/BigDataProject/input_player_data.json'
output_player_data_path = '/home/chirag/Desktop/BigDataProject/output_player_data.json'
input_match_data_path = '/home/chirag/Desktop/BigDataProject/input_match_data.json'
output_match_data_path = '/home/chirag/Desktop/BigDataProject/output_match_data.json'
input_prediction_data_path = '/home/chirag/Desktop/BigDataProject/input_prediction_data.json'
output_prediction_data_path = '/home/chirag/Desktop/BigDataProject/output_prediction_data.json'

#writes the required player profile to a JSON file
def displayPlayerProfile(input_data):
	spark = SparkSession.builder.appName("Write_to_players").getOrCreate()
	sqlcontext=SQLContext(spark)
	players_data_df = sqlcontext.read.json(players_json_path)
	for player in players_data_df.rdd.collect():
		if(player.name == input_data["name"]):
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
			break

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
	
while(1):
	print("__")
	print("MENU")
	print("__")
	print("1. Request details of each player")
	print("2.  Request details of any match")
	print("3. Match Prediction")
	print("ENTER CHOICE")
	print()
	choice=int(input())
	if(choice==1):
		with open(input_player_data_path, "r") as jsonFile:
			input_data = json.load(jsonFile)
		displayPlayerProfile(input_data)
	if(choice==2):
		with open(input_match_data_path, "r") as jsonFile:
			input_data = json.load(jsonFile)
		displayMatchDetails(input_data)
	if(choice==3):
		with open(input_prediction_data_path, "r") as jsonFile:
			input_data = json.load(jsonFile)

		spark = SparkSession.builder.appName("Players CSV DataFrame").getOrCreate()
		sqlcontext=SQLContext(spark)
		df_players= sqlcontext.read.csv(players_csv_hdfs_path,header=True)
		player_name_and_role_columns = ["name","role"]
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
		
		for player_no in team_1:
			player_name = team_1[player_no]
			row_of_player = player_name_and_role_df.filter(player_name_and_role_df.name == player_name)
			player_role = row_of_player.collect()[0].role
			team_1_details[player_role]+=1
		
		for player_no in team_2:
			player_name = team_2[player_no]
			row_of_player = player_name_and_role_df.filter(player_name_and_role_df.name == player_name)
			player_role = row_of_player.collect()[0].role
			team_2_details[player_role]+=1
		
		invalid=0
		if(team_1_details["GK"]!=1 and team_2_details["GK"]!=1):
			invalid=1
		if(team_1_details["DF"]<2 and team_2_details["DF"]<2):
			invalid=1
		if(team_1_details["MD"]<2 and team_2_details["MD"]<2):
			invalid=1
		if(team_1_details["FW"]<1 and team_2_details["FW"<2]):
			invalid=1

		if(invalid==1):
			final_dict = {"result":"Invalid"}
			with open(output_match_data_path) as f:
				f.write(json.dumps(final_dict,indent=4))
			
			
			


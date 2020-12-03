import json
from pyspark.streaming import StreamingContext
import time
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession, Row, SQLContext,DataFrame
from pyspark.sql.types import StructType,StructField,FloatType,StringType,IntegerType
import sys
import findspark
findspark.init()

#paths for players.csv and teams.csv
players_csv_path = 'hdfs://localhost:9000/input/players.csv'
teams_csv_path = 'hdfs://localhost:9000/input/teams.csv'

# stores per match metrics for each player that resets at the end of the match
player_metrics = dict()

conf = SparkConf()
conf.setAppName('BigData Project')

spark_context = SparkContext(conf=conf, master="local[*]")
streaming_context = StreamingContext(spark_context, 2)

streaming_context.checkpoint('BigData Project Checkpoint')
input_stream = streaming_context.socketTextStream('localhost', 6100)

sqlContext = SQLContext(spark_context)

#loading players.csv as a dataframe
players_data_df = sqlContext.read.csv(players_csv_path,header=True)

#loading teams.csv as a dataframe
teams_data_df = sqlContext.read.csv(teams_csv_path,header=True)

#stores the team_id and team_name pair
teams_data=dict()
for team_row in teams_data_df.rdd.collect():
	teams_data[team_row.Id]=team_row.name

#stores players data as a dictionary
players_data=dict()

#stores player ratings as a dictionary
player_ratings=dict()

#stores the pass accuracy for overall stream 
pass_accuracy_details_for_entire_stream=dict()

for player in players_data_df.rdd.collect():
	players_data[player.Id]={
		"Id": player.Id,
		"birthArea": player.birthArea,
		"birthDate": player.birthDate,
		"foot":player.foot,
		"height":player.height,
		"name":player.name,
		"number_of_fouls":0,
		"number_of_goals":0,
		"number_of_matches":0,
		"number_of_own_goals":0,
		"pass_accuracy":0.0,
		"passportArea":player.passportArea,
		"role":player.role,
		"number_of_shots_on_target":0,
		"weight":player.weight,
	}

	player_ratings[player.Id]={
		"0000-00-00":0.5
	}

	pass_accuracy_details_for_entire_stream[player.Id]={
		"numerator":0,
		"denominator":0
	}


players_chemistry = dict()

# Own Goal calculation
own_goal_details = dict()

own_goal = {
	"id": 102
}

def own_goal_calulation(event_record):
	global own_goal_details
	player_id = str(event_record["playerId"])
	tags = event_record["tags"]
	if(len(tags)!=0 and player_id!='0'):
		if(player_id not in own_goal_details.keys()):
			own_goal_details[player_id] = {
				"no_of_own_goals": 0,
			}

		if(own_goal in tags):
			own_goal_details[player_id]["no_of_own_goals"] += 1


# Calculating Pass Accuracy
pass_accuracy_details = dict()

key_pass = {
	"id": 302
}
accurate_pass = {
	"id": 1801
}
inaccurate_pass = {
	"id": 1802
}


def pass_accuracy(event_record):
	global pass_accuracy_details
	global pass_accuracy_details_for_entire_stream
	player_id = str(event_record["playerId"])
	tags = event_record["tags"]
	if(len(tags)!=0 and player_id!='0'):
		if(player_id not in pass_accuracy_details.keys()):
			pass_accuracy_details[player_id] = {
				"no_of_accurate_normal_passes": 0,
				"no_of_inaccurate_normal_passes": 0,
				"no_of_accurate_key_passes": 0,
				"no_of_inaccurate_key_passes": 0,
				"pass_accuracy": 0.0
			}

		if(key_pass in tags and accurate_pass in tags):
			pass_accuracy_details[player_id]["no_of_accurate_key_passes"] += 1
		elif(key_pass in tags and inaccurate_pass in tags):
			pass_accuracy_details[player_id]["no_of_inaccurate_key_passes"] += 1
		elif(key_pass not in tags and accurate_pass in tags):
			pass_accuracy_details[player_id]["no_of_accurate_normal_passes"] += 1
		elif(key_pass not in tags and inaccurate_pass in tags):
			pass_accuracy_details[player_id]["no_of_inaccurate_normal_passes"] += 1

		total_no_of_accurate_normal_passes = pass_accuracy_details[player_id]["no_of_accurate_normal_passes"]
		total_no_of_accurate_key_passes = pass_accuracy_details[player_id]["no_of_accurate_key_passes"]
		total_no_of_normal_passes = pass_accuracy_details[player_id]["no_of_accurate_normal_passes"] + pass_accuracy_details[player_id]["no_of_inaccurate_normal_passes"]
		total_no_of_key_passes = pass_accuracy_details[player_id]["no_of_accurate_key_passes"] + pass_accuracy_details[player_id]["no_of_inaccurate_key_passes"]

		numerator = (total_no_of_accurate_normal_passes + (total_no_of_accurate_key_passes*2))
		denominator = (total_no_of_normal_passes + (total_no_of_key_passes*2))
		
		pass_accuracy_details[player_id]["pass_accuracy"] = numerator/denominator
		
		pass_accuracy_details_for_entire_stream[player_id]["numerator"]+=numerator
		pass_accuracy_details_for_entire_stream[player_id]["denominator"]+=denominator


# duel Effectiveness Calculation
duel_effectiveness_details = dict()

lost_duel = {
	"id": 701
}

neutral_duel = {
	"id": 702
}

won_duel = {
	"id": 703
}

def duel_effectiveness(event_record):
	global duel_effectiveness_details
	player_id = str(event_record["playerId"])
	tags = event_record["tags"]
	if(len(tags)!=0 and player_id!='0'):
		if(player_id not in duel_effectiveness_details.keys()):
			duel_effectiveness_details[player_id] = {
				"no_of_won_duels": 0,
				"no_of_neutral_duels": 0,
				"no_of_lost_duels": 0,
				"duel_effectiveness": 0
			}

		if(won_duel in tags):
			duel_effectiveness_details[player_id]["no_of_won_duels"] += 1
		if(neutral_duel in tags):
			duel_effectiveness_details[player_id]["no_of_neutral_duels"] += 1
		if(lost_duel in tags):
			duel_effectiveness_details[player_id]["no_of_lost_duels"] += 1

		total_no_of_won_duels = duel_effectiveness_details[player_id]["no_of_won_duels"]
		total_no_of_neutral_duels = duel_effectiveness_details[player_id]["no_of_neutral_duels"]
		total_no_of_duels = duel_effectiveness_details[player_id]["no_of_won_duels"] + \
			duel_effectiveness_details[player_id]["no_of_neutral_duels"] + \
			duel_effectiveness_details[player_id]["no_of_lost_duels"]

		duel_effectiveness_details[player_id]["duel_effectiveness"] = (
			(total_no_of_won_duels + (total_no_of_neutral_duels*0.5)) / (total_no_of_duels))


# Free Kick Effectiveness calculation
free_kick_effectiveness_details = dict()

free_kick_effective = {
	"id": 1801
}

free_kick_not_effective = {
	"id": 1802
}

penalty_goal = {
	"id": 101
}

def free_kick_effectiveness(event_record):
	global free_kick_effectiveness_details
	player_id = str(event_record["playerId"])
	tags = event_record["tags"]
	subId = event_record["subEventId"]

	if(len(tags) != 0 and player_id!='0'):
		if(player_id not in free_kick_effectiveness_details.keys()):
			free_kick_effectiveness_details[player_id] = {
				"no_of_effective_free_kicks": 0,
				"no_of_not_effective_free_kicks": 0,
				"no_of_penalties_scored": 0,
				"free_kick_effectiveness": 0
			}

		if(free_kick_effective in tags and subId != 35):
			free_kick_effectiveness_details[player_id]["no_of_effective_free_kicks"] += 1
		if(free_kick_not_effective in tags and subId != 35):
			free_kick_effectiveness_details[player_id]["no_of_not_effective_free_kicks"] += 1
		if(free_kick_not_effective in tags and subId == 35):
			free_kick_effectiveness_details[player_id]["no_of_not_effective_free_kicks"] += 1
		if(free_kick_effective in tags and subId == 35):
			if(penalty_goal in tags):
				free_kick_effectiveness_details[player_id]["no_of_effective_free_kicks"] += 1
				free_kick_effectiveness_details[player_id]["no_of_penalties_scored"] += 1
			if(penalty_goal not in tags):
				free_kick_effectiveness_details[player_id]["no_of_effective_free_kicks"] += 1
		if(free_kick_effective in tags and subId!=35):
			free_kick_effectiveness_details[player_id]["no_of_effective_free_kicks"] += 1

		total_no_of_effective_free_kicks = free_kick_effectiveness_details[
			player_id]["no_of_effective_free_kicks"]
		total_no_of_penalties_scored = free_kick_effectiveness_details[
			player_id]["no_of_penalties_scored"]
		total_no_of_free_kicks = free_kick_effectiveness_details[player_id]["no_of_effective_free_kicks"] + \
			free_kick_effectiveness_details[player_id]["no_of_not_effective_free_kicks"]

		free_kick_effectiveness_details[player_id]["free_kick_effectiveness"] = (
			total_no_of_effective_free_kicks + total_no_of_penalties_scored) / (total_no_of_free_kicks)


# Shot Effectiveness calculation
shot_effectiveness_details = dict()

shot_on_target = {
	"id": 1801
}

shot_not_on_target = {
	"id": 1802
}

shot_goal = {
	"id": 101
}

def shot_effectiveness(event_record):
	global shot_effectiveness_details
	player_id = str(event_record["playerId"])
	tags = event_record["tags"]
	if(len(tags)!=0 and player_id!='0'):
		if(player_id not in shot_effectiveness_details.keys()):
			shot_effectiveness_details[player_id] = {
				"no_shots_on_target_and_not_goal": 0,
				"no_shots_on_target_and_goal": 0,
				"no_shots_not_on_target": 0,
				"shot_effectiveness": 0
			}

		if(shot_goal in tags and shot_on_target):
			shot_effectiveness_details[player_id]["no_shots_on_target_and_goal"] += 1
		if(shot_on_target in tags and shot_goal not in tags):
			shot_effectiveness_details[player_id]["no_shots_on_target_and_not_goal"] += 1
		if(shot_not_on_target in tags):
			shot_effectiveness_details[player_id]["no_shots_not_on_target"] += 1

		total_no_of_shots_on_target_and_goals = shot_effectiveness_details[
			player_id]["no_shots_on_target_and_goal"]
		total_no_of_shots_on_target_but_not_goals = shot_effectiveness_details[
			player_id]["no_shots_on_target_and_not_goal"]
		total_no_of_shots = shot_effectiveness_details[player_id]["no_shots_on_target_and_goal"] + \
			shot_effectiveness_details[player_id]["no_shots_on_target_and_not_goal"] + \
			shot_effectiveness_details[player_id]["no_shots_not_on_target"]

		shot_effectiveness_details[player_id]["shot_effectiveness"] = (
			(total_no_of_shots_on_target_and_goals) + (total_no_of_shots_on_target_but_not_goals*0.5)) / (total_no_of_shots)


# Foul Loss calculation
foul_details = dict()

def foul_loss(event_record):
	global foul_details
	player_id = str(event_record["playerId"])

	if(player_id not in foul_details.keys() and player_id!='0'):
		foul_details[player_id] = {
			"no_of_fouls": 0,
		}

	foul_details[player_id]["no_of_fouls"] += 1

#stores the current_match object
current_match = None

#stores details of all matches
match_details = dict()

#stores end of match metrics like player contribution,player performance which reset every match
end_of_match_player_metrics = dict()

#updates players data after every match
def update_end_of_stream_playersdata(players_in_this_match):
	global players_data
	global player_metrics
	for player_id in players_in_this_match:
		players_data[player_id]["number_of_fouls"]+= player_metrics[player_id]["number_of_fouls"]
		players_data[player_id]["number_of_goals"]+= player_metrics[player_id]["number_of_goals"]
		players_data[player_id]["number_of_own_goals"]+= player_metrics[player_id]["number_of_own_goals"]
		players_data[player_id]["number_of_shots_on_target"]+=player_metrics[player_id]["no_of_shots_on_target"]
		if(players_in_this_match[player_id]["minutesPlayed"] > 0):
			players_data[player_id]["number_of_matches"]+=1

#Displays match result as "DRAW" if team_id is '0' else displays winner team name
def displayWinner(team_id):
	global teams_data
	if(team_id=='0'):
		return("DRAW")
	else:
		return(teams_data[team_id])

# Updates the player chemistries
def updatePlayerChemistries(temp_player_ratings, team_1_players, team_2_players):
	global players_chemistry

	team_1_players_ids = []
	for obj in team_1_players:
		team_1_players_ids.append(str(obj["playerId"]))

	team_2_players_ids = []
	for obj in team_2_players:
		team_2_players_ids.append(str(obj["playerId"]))

	for i in range(len(team_1_players_ids+team_2_players_ids)):
		player_id_1 = (team_1_players_ids+team_2_players_ids)[i]
		for j in range(i+1,len(team_1_players_ids+team_2_players_ids)):
			player_id_2 = (team_1_players_ids+team_2_players_ids)[j]
			if((player_id_1 != player_id_2) and ((player_id_1,player_id_2) not in players_chemistry)):
				players_chemistry[(player_id_1,player_id_2)] = 0.5
				players_chemistry[(player_id_2,player_id_1)] = 0.5

	for i in range(len(team_1_players_ids+team_2_players_ids)):
		player_id_1 = (team_1_players_ids+team_2_players_ids)[i]
		for j in range(i+1, len(team_1_players_ids+team_2_players_ids)):
			player_id_2 = (team_1_players_ids+team_2_players_ids)[j]
			if(player_id_1 != player_id_2):
				if((player_id_1 in team_1_players_ids and player_id_2 in team_1_players_ids) or (player_id_1 in team_2_players_ids and player_id_2 in team_2_players_ids)):
					change_in_chemistry = abs(
						(temp_player_ratings[player_id_1]["change_in_rating"]+temp_player_ratings[player_id_2]["change_in_rating"])/2)
					if((temp_player_ratings[player_id_1]["change_in_rating"] >= 0 and temp_player_ratings[player_id_2]["change_in_rating"] >= 0) or (temp_player_ratings[player_id_1]["change_in_rating"] < 0 and temp_player_ratings[player_id_2]["change_in_rating"] < 0)):
						players_chemistry[(player_id_1,player_id_2)]+= change_in_chemistry
						players_chemistry[(player_id_2,player_id_1)]+= change_in_chemistry

					else:
						players_chemistry[(player_id_1,player_id_2)]-= change_in_chemistry
						players_chemistry[(player_id_2,player_id_1)]-= change_in_chemistry
				elif((player_id_1 in team_1_players_ids and player_id_2 in team_2_players_ids) or (player_id_2 in team_1_players_ids and player_id_1 in team_2_players_ids)):
					change_in_chemistry = abs(
						(temp_player_ratings[player_id_1]["change_in_rating"]+temp_player_ratings[player_id_2]["change_in_rating"])/2)
					if((temp_player_ratings[player_id_1]["change_in_rating"] >= 0 and temp_player_ratings[player_id_2]["change_in_rating"] >= 0) or temp_player_ratings[player_id_1]["change_in_rating"] < 0 and temp_player_ratings[player_id_2]["change_in_rating"] < 0):
						players_chemistry[(player_id_1,player_id_2)]-= change_in_chemistry
						players_chemistry[(player_id_2,player_id_1)]-= change_in_chemistry
					else:
						players_chemistry[(player_id_1,player_id_2)]+= change_in_chemistry
						players_chemistry[(player_id_2,player_id_1)]+= change_in_chemistry

# Find the player contribution in a match
def findPlayerContribution(player_id, players_in_this_match):
	global player_metrics
	if(players_in_this_match[player_id]["minutesPlayed"] == 90):
		return(1.05*((player_metrics[player_id]["pass_accuracy"]+player_metrics[player_id]["duel_effectiveness"]+player_metrics[player_id]["free_kick_effectiveness"]+player_metrics[player_id]["no_of_shots_on_target"]))/4)
	if(players_in_this_match[player_id]["minutesPlayed"] != 90 and players_in_this_match[player_id]["minutesPlayed"] != 0):
		return(players_in_this_match[player_id]["minutesPlayed"]/90*((player_metrics[player_id]["pass_accuracy"]+player_metrics[player_id]["duel_effectiveness"]+player_metrics[player_id]["free_kick_effectiveness"]+player_metrics[player_id]["no_of_shots_on_target"]))/4)
	if(players_in_this_match[player_id]["minutesPlayed"] == 0):
		return(0)

# Finds the details of yellow cards in the match
def FindMatchYellowCards(team_1_players, team_2_players):
	yelow_card_players = []
	global players_data
	for player in team_1_players+team_2_players:
		if(int(player["yellowCards"]) > 0):
			yelow_card_players.append(players_data[str(player["playerId"])]["name"])
	return(yelow_card_players)

# Finds the details of red cards in the match
def FindMatchRedCards(team_1_players, team_2_players):
	red_card_players = []
	global players_data
	for player in team_1_players+team_2_players:
		if(int(player["redCards"]) > 0):
			red_card_players.append(players_data[str(player["playerId"])]["name"])
	return(red_card_players)

# Finds details of own goals in the match
def FindMatchOwnGoals(team_1_id, team_2_id, team_1_players, team_2_players):
	own_goals = []
	global players_data
	global teams_data

	for player_details_in_match in team_1_players:
		if(int(player_details_in_match["ownGoals"]) > 0):
			own_goals.append(
				{
					"player_name": players_data[str(player_details_in_match["playerId"])]["name"],
					"team_name": teams_data[team_1_id],
					"number_of_goals": int(player_details_in_match["ownGoals"])
				}
			)

	for player_details_in_match in team_2_players:
		if(int(player_details_in_match["ownGoals"]) > 0):
			own_goals.append(
				{
					"player_name": players_data[str(player_details_in_match["playerId"])]["name"],
					"team_name": teams_data[team_2_id],
					"number_of_goals": int(player_details_in_match["ownGoals"])
				}
			)
	return(own_goals)

# Finds details of goals scored in the match
def FindMatchGoals(team_1_id, team_2_id, team_1_players, team_2_players):
	goals = []

	for player_details_in_match in team_1_players:
		gl=player_details_in_match["goals"]
		if(gl!='null' and int(gl) > 0):
			goals.append(
				{
					"player_name": players_data[str(player_details_in_match["playerId"])]["name"],
					"team_name": teams_data[team_1_id],
					"number_of_goals": int(player_details_in_match["goals"])
				}
			)

	for player_details_in_match in team_2_players:
		gl=player_details_in_match["goals"]
		if(gl!='null' and int(gl) > 0):
			goals.append(
				{
					"player_name": players_data[str(player_details_in_match["playerId"])]["name"],
					"team_name": teams_data[team_2_id],
					"number_of_goals": int(player_details_in_match["goals"])
				}
			)
	return(goals)

# Runs after each match
def end_of_match_calculation(match_record):
	global player_metrics
	global match_details
	global end_of_match_player_metrics
	global player_ratings
	global pass_accuracy_details
	global duel_effectiveness_details
	global free_kick_effectiveness_details
	global shot_effectiveness_details
	global own_goal_details
	global foul_details
	global teams_data
	
	match_date = match_record["dateutc"][0:10]

	teams_ids = []
	for team_id in match_record["teamsData"].keys():
		teams_ids.append(team_id)

	team_1_id = teams_ids[0]
	team_2_id = teams_ids[1]

	team_1_formation = match_record["teamsData"][team_1_id]["formation"]
	team_2_formation = match_record["teamsData"][team_2_id]["formation"]

	team_1_players = team_1_formation["lineup"] + team_1_formation["bench"]
	team_2_players = team_2_formation["lineup"] + team_2_formation["bench"]

	team_1_lineup = team_1_formation["lineup"]
	team_2_lineup = team_2_formation["lineup"]

	substitutions = team_1_formation["substitutions"] + \
		team_2_formation["substitutions"]

	# players_in_this_match holds info about time each player spent on the field in the current match
	players_in_this_match = dict()

	# initially sets time played as 0 for all players
	for player in team_1_players+team_2_players:
		players_in_this_match[str(player["playerId"])] = {
			"minutesPlayed": 0
		}

	# then for the players in the playing 11 (lineup), we set the time played as 90 mins
	for player in team_1_lineup+team_2_lineup:
		players_in_this_match[str(player["playerId"])] = {
			"minutesPlayed": 90
		}

	# then for the players who were substituted in or out,we update the time played accordingly
	for subs_obj in substitutions:
		players_in_this_match[str(subs_obj["playerIn"])] = {
			"minutesPlayed": 90-subs_obj["minute"]
		}
		players_in_this_match[str(subs_obj["playerOut"])] = {
			"minutesPlayed": subs_obj["minute"]
		}

	# initially players_metrics only holds metric details of players who were involved in an event.
	# so for players who havent been involved in an event,we set their metric values to 0
	for player_id in players_in_this_match:
		if(player_id not in player_metrics):
			player_metrics[player_id] = {
				"pass_accuracy": 0.0,
				"duel_effectiveness": 0,
				"free_kick_effectiveness": 0,
				"shot_effectiveness": 0,
				"number_of_fouls": 0,
				"number_of_own_goals": 0,
				"no_of_shots_on_target": 0,
				"number_of_goals": 0
			}

	# match_details holds info about all matches that have been played so far
	match_id = str(match_record["wyId"])
	match_details[match_id] = {
		"label":match_record["label"],
		"date": match_record["dateutc"][0:10],
		"duration": match_record["duration"],
		"winner": displayWinner(str(match_record["winner"])),
		"venue": match_record["venue"],
		"gameweek": match_record["gameweek"],
		"goals": FindMatchGoals(team_1_id, team_2_id, team_1_players, team_2_players),
		"own_goals": FindMatchOwnGoals(team_1_id, team_2_id, team_1_players, team_2_players),
		"yellow_cards": FindMatchYellowCards(team_1_players, team_2_players),
		"red_cards": FindMatchRedCards(team_1_players, team_2_players)
	}

	#stores the old and new ratings of each player which is needed to calculate chemistry between players
	temp_player_ratings = dict()

	# Finds the player contribution,player performance and updates the player rating based on the performance in the current match
	for player_id in players_in_this_match:
		end_of_match_player_metrics[player_id] = {
			"player_contribution": findPlayerContribution(player_id, players_in_this_match)
		}
		end_of_match_player_metrics[player_id]["player_performance"] = end_of_match_player_metrics[player_id]["player_contribution"]-(
			0.0005*player_metrics[player_id]["number_of_fouls"])-(0.05*player_metrics[player_id]["number_of_own_goals"])

		previous_match_date = list(player_ratings[player_id].keys())[-1]

		temp_player_ratings[player_id] = {
			"old_rating": player_ratings[player_id][previous_match_date]
		}

		player_ratings[player_id][match_date]=(player_ratings[player_id][previous_match_date] + end_of_match_player_metrics[player_id]["player_performance"])/2
		temp_player_ratings[player_id]["new_rating"] = player_ratings[player_id][match_date]
		temp_player_ratings[player_id]["change_in_rating"] = temp_player_ratings[player_id]["new_rating"] - \
			temp_player_ratings[player_id]["old_rating"]

	update_end_of_stream_playersdata(players_in_this_match)
	updatePlayerChemistries(temp_player_ratings,team_1_players, team_2_players)

	# Clears all the dictionaries as these hold per match data
	temp_player_ratings.clear()
	player_metrics.clear()
	end_of_match_player_metrics.clear()
	pass_accuracy_details.clear()
	duel_effectiveness_details.clear()
	free_kick_effectiveness_details.clear()
	shot_effectiveness_details.clear()
	own_goal_details.clear()
	foul_details.clear()

# Finds the no of goals the player scored upto the current event in the match
def findNumberOfGoals(player_id):
	global shot_effectiveness_details
	global free_kick_effectiveness_details

	if(player_id not in shot_effectiveness_details.keys() and player_id not in free_kick_effectiveness_details.keys()):
		return(0)
	elif(player_id in shot_effectiveness_details.keys() and player_id not in free_kick_effectiveness_details.keys()):
		return(shot_effectiveness_details[player_id]["no_shots_on_target_and_goal"])
	elif(player_id not in shot_effectiveness_details.keys() and player_id in free_kick_effectiveness_details.keys()):
		return(free_kick_effectiveness_details[player_id]["no_of_penalties_scored"])
	else:
		return((shot_effectiveness_details[player_id]["no_shots_on_target_and_goal"] + free_kick_effectiveness_details[player_id]["no_of_penalties_scored"]))

# Runs on every batch RDD
def metrics_calculation(a):
	global current_match
	global player_metrics
	global pass_accuracy_details
	global shot_effectiveness_details
	global free_kick_effectiveness_details
	global own_goal_details,foul_details
	global foul_details
	global duel_effectiveness_details
	for i in a.collect():
		#if object is a match object,store it in current_match
		if("status" in json.loads(i).keys()):
			current_match = json.loads(i)

		player_id = ""
		if("eventId" in json.loads(i).keys()):
			player_id = str(json.loads(i)["playerId"])
		if(player_id!='0'):
			if("eventId" in json.loads(i).keys() and json.loads(i)["eventId"] == 8):
				pass_accuracy(json.loads(i))
			if("eventId" in json.loads(i).keys() and json.loads(i)["eventId"] == 1):
				duel_effectiveness(json.loads(i))
			if("eventId" in json.loads(i).keys() and json.loads(i)["eventId"] == 3):
				free_kick_effectiveness(json.loads(i))
			if("eventId" in json.loads(i).keys() and json.loads(i)["eventId"] == 2):
				foul_loss(json.loads(i))
			if("eventId" in json.loads(i).keys() and json.loads(i)["eventId"] == 10):
				shot_effectiveness(json.loads(i))
			if("eventId" in json.loads(i).keys()):
				own_goal_calulation(json.loads(i))

		#checking !=0 was because there was an error with a stream object that had a player_id as 0 incorrectly
		if(player_id != '0' and player_id):
			player_metrics[player_id] = {
				"pass_accuracy": 0.0 if player_id not in pass_accuracy_details.keys() else pass_accuracy_details[player_id]["pass_accuracy"],
				"duel_effectiveness": 0 if player_id not in duel_effectiveness_details.keys() else duel_effectiveness_details[player_id]["duel_effectiveness"],
				"free_kick_effectiveness": 0 if player_id not in free_kick_effectiveness_details.keys() else free_kick_effectiveness_details[player_id]["free_kick_effectiveness"],
				"shot_effectiveness": 0 if player_id not in shot_effectiveness_details.keys() else shot_effectiveness_details[player_id]["shot_effectiveness"],
				"number_of_fouls": 0 if player_id not in foul_details.keys() else foul_details[player_id]["no_of_fouls"],
				"number_of_own_goals": 0 if player_id not in own_goal_details.keys() else own_goal_details[player_id]["no_of_own_goals"],
				"no_of_shots_on_target": 0 if player_id not in shot_effectiveness_details.keys() else (shot_effectiveness_details[player_id]["no_shots_on_target_and_goal"] + shot_effectiveness_details[player_id]["no_shots_on_target_and_not_goal"]),
				"number_of_goals":  findNumberOfGoals(player_id)
			}

	#to calculate end of match metrics only if there is data in the batch RDD.
	if(len(a.collect()) != 0):
		end_of_match_calculation(current_match)

#writes the final end of stream pass_accuracy values to players data which will be finally written to HDFS
def writeFinalPassAccuracyToPlayersData():
	global pass_accuracy_details_for_entire_stream
	global players_data

	for player_id in players_data:
		if(pass_accuracy_details_for_entire_stream[player_id]["denominator"]!=0):
			players_data[player_id]["pass_accuracy"]=(pass_accuracy_details_for_entire_stream[player_id]["numerator"])/(pass_accuracy_details_for_entire_stream[player_id]["denominator"])

#writes data to HDFS
def writeToHDFS():
	global players_data
	global player_ratings
	global match_details
	global players_chemistry

	spark = SparkSession.builder.appName("Write to HDFS").getOrCreate()
	sqlContext=SQLContext(spark)

	#writing players_data to hdfs
	lst1=[]
	for d in players_data.keys():
		lst1.append(players_data[d])

	df1 = sqlContext.createDataFrame(lst1)
	df1.write.json("/input_proj/players2.json",mode = "overwrite")

	#writing player_ratings to hdfs
	l=[]
	for player_id in player_ratings:
		for date in player_ratings[player_id]:
			b=((player_id,date),player_ratings[player_id][date])
			l.append(b)

	player_rating_schema = StructType([
		StructField('name', StructType([
			 StructField('player_id', StringType(), True),
			 StructField('date', StringType(), True),
			 ])),
		 StructField('rating', FloatType(), True),
		 ])

	df2 = sqlContext.createDataFrame(data=l,schema=player_rating_schema)
	df2.write.json("/input_proj/player_ratings.json",mode = "overwrite")

	#writing player chemistries to hdfs
	l=[]
	for (player_id_1,player_id_2) in players_chemistry:
		b=((player_id_1,player_id_2),players_chemistry[(player_id_1,player_id_2)])
		l.append(b)

	players_chemistry_schema = StructType([
		StructField('(player1,player2)', StructType([
			 StructField('player_id_1', StringType(), True),
			 StructField('player_id_2', StringType(), True),
			 ])),
		 StructField('chemistry', FloatType(), True),
		 ])

	df3 = sqlContext.createDataFrame(data=l,schema=players_chemistry_schema)
	df3.write.json("/input_proj/players_chemistry.json",mode = "overwrite")

	#writing match_details to local file
	with open('/home/chirag/Desktop/BigDataProject/matches_details.json','w') as f:
		f.write(json.dumps(match_details,indent=4))


input_stream.foreachRDD(lambda a:metrics_calculation(a))
#input_stream.pprint()

streaming_context.start()
streaming_context.awaitTermination(350)
streaming_context.stop()
writeFinalPassAccuracyToPlayersData()
writeToHDFS()
pass_accuracy_details_for_entire_stream.clear()
players_data.clear()
teams_data.clear()

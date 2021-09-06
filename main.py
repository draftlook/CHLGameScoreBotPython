import requests
import pandas as pd
import json
import time
from random import randint
import itertools
import numpy as np
from collections import Counter
import operator
from bs4 import BeautifulSoup
import dataframe_image as dfi
import tweepy
import os

##Function: Get daily game urls for OHL, WHL, and QMJHL
def get_daily_urls(date):
    #Setting league keys
    ohl_key = "2976319eb44abe94"
    whl_key = "41b145a848f4bd67"
    lhjmq_key = "f322673b6bcae299"
    #Getting OHL games
    ohl_jsonurl = "https://lscluster.hockeytech.com/feed/?feed=modulekit&view=gamesbydate&key=" + ohl_key + "&fmt=json&client_code=ohl" + "&lang=en&league_code=&fetch_date=" + date + "&fmt=json"
    ohl_resp = requests.get(ohl_jsonurl)
    ohl_json = ohl_resp.json()
    ohl_game_urls = []
    for i in range(len(ohl_json['SiteKit']['Gamesbydate'])):
        ohl_game_urls.append("https://ontariohockeyleague.com/gamecentre/" + ohl_json['SiteKit']['Gamesbydate'][i]['id'])
    #Getting WHL games
    whl_jsonurl = "https://lscluster.hockeytech.com/feed/?feed=modulekit&view=gamesbydate&key=" + whl_key + "&fmt=json&client_code=whl" + "&lang=en&league_code=&fetch_date=" + date + "&fmt=json"
    whl_resp = requests.get(whl_jsonurl)
    whl_json = whl_resp.json()
    whl_game_urls = []
    for i in range(len(whl_json['SiteKit']['Gamesbydate'])):
        whl_game_urls.append("https://whl.ca/gamecentre/" + whl_json['SiteKit']['Gamesbydate'][i]['id'])
    #Getting QMJHL games
    qmjhl_jsonurl = "https://lscluster.hockeytech.com/feed/?feed=modulekit&view=gamesbydate&key=" + lhjmq_key + "&fmt=json&client_code=lhjmq" + "&lang=en&league_code=&fetch_date=" + date + "&fmt=json"
    qmjhl_resp = requests.get(qmjhl_jsonurl)
    qmjhl_json = qmjhl_resp.json()
    qmjhl_game_urls = []
    for i in range(len(qmjhl_json['SiteKit']['Gamesbydate'])):
        qmjhl_game_urls.append("https://theqmjhl.ca/gamecentre/" + qmjhl_json['SiteKit']['Gamesbydate'][i]['id'])
    return ohl_game_urls + whl_game_urls + qmjhl_game_urls

##Function: Get full lineup info/statistics
def tweet_game_stats(gameurl):
    ## Building JSON link
    #Send a request to game url
    resp = requests.get(gameurl)
    #Getting HTML content
    soup = BeautifulSoup(resp.content)
    #Extracting key
    key = soup.main.div['data-feed_key']
    #Extracting league
    league = soup.main.div['data-league']
    #Extracting path
    path = soup.main.div['data-path']
    #Extracting lang
    lang = soup.main.div['data-lang']
    #Formulating JSON url
    jsonurl = "https://cluster.leaguestat.com/feed/index.php?feed=gc&key=" + key + "&client_code=" + league + "&game_id=" + path + "&lang_code=" + lang + "&fmt=json&tab=gamesummary"

    # Send a request to this URL to pull data
    response = requests.get(jsonurl)
    # System sleep between attempts
    time.sleep(randint(4, 5))
    # Pull JSON data from URL
    game_data = response.json()
    # Get home and away lineups
    home_data = game_data['GC']['Gamesummary']['home_team_lineup']['players']
    away_data = game_data['GC']['Gamesummary']['visitor_team_lineup']['players']
    # Convert JSON to a pandas dataframe
    home_lineup = pd.DataFrame(home_data)
    away_lineup = pd.DataFrame(away_data)
    # Get date
    date = game_data['GC']['Gamesummary']['meta']['date_played']
    # Get team names and abbreviations
    home_team = game_data['GC']['Gamesummary']['home']['name'].replace(",", "")
    home_code = game_data['GC']['Gamesummary']['home']['team_code'].upper()
    home_score = game_data['GC']['Gamesummary']['meta']['home_goal_count']
    away_team = game_data['GC']['Gamesummary']['visitor']['name'].replace(",", "")
    away_code = game_data['GC']['Gamesummary']['visitor']['team_code'].upper()
    away_score = game_data['GC']['Gamesummary']['meta']['visiting_goal_count']

    # Get game id and date
    date = game_data['GC']['Gamesummary']['meta']['date_played']
    gameid = game_data['GC']['Gamesummary']['meta']['id']

    # Adding home/away, team code, game id, game date, opponent team, and home/away goal count columns to dataframes
    full_lnuph = pd.DataFrame(data={'H_A': ['H'], 'team': [home_code], 'opp_team': [away_code],
                                    'date': date,
                                    'gameid': [game_data['GC']['Gamesummary']['meta']['id']],
                                    'team_goals': [game_data['GC']['Gamesummary']['meta']['home_goal_count']]})
    final_lineup_home = home_lineup.assign(**full_lnuph.iloc[0])
    full_lnupa = pd.DataFrame(data={'H_A': ['A'], 'team': [away_code], 'opp_team': [home_code],
                                    'date': date,
                                    'gameid': [game_data['GC']['Gamesummary']['meta']['id']],
                                    'team_goals': [game_data['GC']['Gamesummary']['meta']['visiting_goal_count']]})
    final_lineup_away = away_lineup.assign(**full_lnupa.iloc[0])

    # Identifying df columns to keep
    col_list = ['gameid', 'date', 'player_id', 'first_name', 'last_name', 'position_str', 'goals', 'assists',
                'plusminus',
                'pim', 'faceoff_wins', 'faceoff_attempts', 'shots', 'shots_on', 'team', 'opp_team', 'H_A', 'team_goals']
    # Combining home and away dataframes
    finallineup = final_lineup_home.append(final_lineup_away)
    # Selecting df columns to keep
    finallineup = finallineup[col_list]

    # Pulling goal data from JSON file
    goals_list = game_data['GC']['Gamesummary']['goals']
    # Creating a dataframe of goal data
    goalsdf = pd.DataFrame(goals_list)
    # Getting scorer_ids and goal counts
    scorer_ids = Counter([d.get('player_id') for d in goalsdf.goal_scorer])
    # Creating a dataframe of scorer ids and goal counts
    scorersdf = pd.DataFrame.from_dict(scorer_ids, orient='index', columns=['goal_count']).reset_index()
    # Getting first assist ids and first assist counts
    firstassist_ids = Counter([d.get('player_id') for d in goalsdf.assist1_player])
    # Creating a dataframe of first assist ids and first assist counts
    firstassistdf = pd.DataFrame.from_dict(firstassist_ids, orient='index', columns=['firstassist_count']).reset_index()
    # Getting second assist ids and second assist counts
    secondassist_ids = Counter([d.get('player_id') for d in goalsdf.assist2_player])
    # Creating a dataframe of second assist ids and second assist counts
    secondassistdf = pd.DataFrame.from_dict(secondassist_ids, orient='index',columns=['secondassist_count']).reset_index()

    # Creating a list that specifies whether a goal counts towards plus/minus or not
    fvf_temp = []
    for i in range(len(goalsdf)):
        if (goalsdf.power_play[i] == '0' and goalsdf.empty_net[i] == '0' and goalsdf.short_handed[i] == '0' and
                goalsdf.penalty_shot[i] == '0'):
            fvf_temp.append(i)

    # Getting pluses and creating a counter dataframe with ids and plus counts
    plus_temp = []
    for i in fvf_temp:
        plus_temp.append([d.get('player_id') for d in game_data['GC']['Gamesummary']['goals'][i]['plus']])
    plus_ids = Counter(list(itertools.chain(*plus_temp)))
    plusdf = pd.DataFrame.from_dict(plus_ids, orient='index', columns=['gf_count']).reset_index()

    # Getting minuses and creating a counter dataframe with ids and minus counts
    minus_temp = []
    for i in fvf_temp:
        minus_temp.append([d.get('player_id') for d in game_data['GC']['Gamesummary']['goals'][i]['minus']])
    minus_ids = Counter(list(itertools.chain(*minus_temp)))
    minusdf = pd.DataFrame.from_dict(minus_ids, orient='index', columns=['ga_count']).reset_index()

    # Getting penalties taken and creating a counter dataframe with ids and penalty taken counts
    pen_temp = []
    for i in range(len(game_data['GC']['Gamesummary']['penalties'])):
        pen_temp.append(game_data['GC']['Gamesummary']['penalties'][i]['player_penalized_info'].get('player_id', None))
    pen_ids = Counter(pen_temp)
    pendf = pd.DataFrame.from_dict(pen_ids, orient='index', columns=['pen_count']).reset_index()

    # Merging totals into lineup dataframe
    fulldf = pd.merge(finallineup, scorersdf, how='left', left_on='player_id', right_on='index')
    fulldf = pd.merge(fulldf, firstassistdf, how='left', left_on='player_id', right_on='index')
    fulldf = pd.merge(fulldf, secondassistdf, how='left', left_on='player_id', right_on='index')
    fulldf = pd.merge(fulldf, plusdf, how='left', left_on='player_id', right_on='index')
    fulldf = pd.merge(fulldf, minusdf, how='left', left_on='player_id', right_on='index')
    fulldf = pd.merge(fulldf, pendf, how='left', left_on='player_id', right_on='index')
    # Replacing Nan with 0
    fulldf = fulldf.fillna(0)
    # Converting count columns to integer
    fulldf['goal_count'] = fulldf['goal_count'].astype(int)
    fulldf['firstassist_count'] = fulldf['firstassist_count'].astype(int)
    fulldf['secondassist_count'] = fulldf['secondassist_count'].astype(int)
    fulldf['gf_count'] = fulldf['gf_count'].astype(int)
    fulldf['ga_count'] = fulldf['ga_count'].astype(int)
    fulldf['pen_count'] = fulldf['pen_count'].astype(int)
    fulldf['faceoff_wins'] = fulldf['faceoff_wins'].astype(int)
    fulldf['faceoff_attempts'] = fulldf['faceoff_attempts'].astype(int)
    fulldf['shots_on'] = fulldf['shots_on'].astype(int)
    # Adding faceoff losses column and full name column
    fulldf = fulldf.assign(faceoff_losses=fulldf.faceoff_attempts - fulldf.faceoff_wins)
    fulldf = fulldf.assign(name=fulldf.first_name + " " + fulldf.last_name)
    # Selecting final dataframe columns
    finaldf = fulldf[
        ['gameid', 'date', 'opp_team', 'player_id', 'name', 'team', 'H_A', 'team_goals', 'position_str', 'goal_count',
         'firstassist_count', 'secondassist_count', 'faceoff_wins', 'faceoff_losses', 'shots', 'shots_on', 'gf_count',
         'ga_count', 'pen_count']]
    # Calculating and adding a game score column
    finaldf = finaldf.assign(
        gamescore=(finaldf.goal_count * 0.75) + (finaldf.firstassist_count * 0.7) + (
                    finaldf.secondassist_count * 0.55) + (
                          finaldf.shots_on * 0.075) + (finaldf.pen_count * -0.15) + (finaldf.faceoff_wins * 0.01) + (
                          finaldf.faceoff_losses * -0.01) + (finaldf.gf_count * 0.15) + (finaldf.ga_count * -0.15))
    # Selecting tabledf columns to keep
    tabledf = finaldf[
        ['name', 'position_str', 'team', 'goal_count', 'firstassist_count', 'secondassist_count', 'shots_on',
         'pen_count', 'faceoff_wins', 'faceoff_losses', 'gf_count', 'ga_count', 'gamescore']]
    # Renaming tabledf columns
    tabledf = tabledf.rename(
        columns={'name': 'Player', 'position_str': 'Pos', 'team': 'Team', 'goal_count': 'G', 'firstassist_count': 'A1',
                 'secondassist_count': 'A2', 'shots_on': 'SOG', 'pen_count': 'Pen.', 'faceoff_wins': 'FOW',
                 'faceoff_losses': 'FOL', 'gf_count': 'GF', 'ga_count': 'GA', 'gamescore': 'GameScore'})
    #Sorting tabledf by game score
    tabledf = tabledf.sort_values(by='GameScore', ascending=False)
    # Resetting index for tabledf
    tabledf.reset_index(drop=True, inplace=True)
    # Saving tabledf as a png to filepath "temp.png"
    dfi.export(tabledf, "temp.png")
    # Authenticating to Twitter
    auth = tweepy.OAuthHandler(os.environ['API_KEY'], os.environ['API_SECRET'])
    auth.set_access_token(os.environ['ACCESS_TOKEN'], os.environ['ACCESS_SECRET'])
    # Creating API object
    api = tweepy.API(auth)
    # Creating tweet string
    tweetstring = "#CHL Game Score Card: " + home_team + " (" + home_score + ") " + "vs " + away_team + " (" + away_score + ") on " + date
    # Posting tweet
    api.update_status(tweetstring, media_ids=[api.media_upload("temp.png").media_id])
    return(tabledf)

# Setting date
date = "2019-09-29"
# Execute function get_daily_urls to get all game urls for the day's games
game_urls = get_daily_urls(date)
# Execute function tweet_game_stats for each url in game_urls
game_stats = []
for i in game_urls: game_stats.append(tweet_game_stats(i))

# Combining all dfs in game_stats into one single df
all_games = pd.concat(game_stats).sort_values(by='GameScore', ascending=False)
# Resetting index for all_games df
all_games.reset_index(drop=True, inplace=True)
# Selecting the top ten rows of all_games
top_games = all_games.head(10)
# Saving all_games as png at filepath "top_performers_temp.png"
dfi.export(top_games, "top_performers_temp.png")
# Authenticating to Twitter
auth = tweepy.OAuthHandler(os.environ['API_KEY'], os.environ['API_SECRET'])
auth.set_access_token(os.environ['ACCESS_TOKEN'], os.environ['ACCESS_SECRET'])
# Creating API object
api = tweepy.API(auth)
# Creating tweet string
tweetstring = "#CHL Top Performers on " + date
# Posting tweet
api.update_status(tweetstring, media_ids=[api.media_upload("top_performers_temp.png").media_id])


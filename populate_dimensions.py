# Populate Dimensions - 'dim_players', 'dim_officials', 'dim_seasons', 'dim_teams', 'dim_stadium'

# Importing All Libraries
import json
import pandas as pd
from sqlalchemy import create_engine
import glob
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from geopy.geocoders import Nominatim
from translate import Translator

# Function to Get Country name
def get_country(stadium_name):
    geolocator = Nominatim(user_agent="MyApp")
    try:
        location = geolocator.geocode(stadium_name)
        latitude = str(location.latitude)
        longitude = str(location.longitude)
        location = geolocator.reverse(latitude+","+longitude)
        address = location.raw['address']
        country = address["country"]
        country_code  = address["country_code"]
        if country_code == "ae":
            translator= Translator(from_lang="arabic",to_lang="english")
            translation = translator.translate(country)
            country = translation
        else:
            country = country
    except Exception:
        country = ""
    return country

# Loading all files from the IPL Folder
all_files = glob.glob("C:/users/amirajkar/Downloads/ipl_json/*")
all_files.sort()

# Creating Empty Lists to Load the Data
officials = []
all_players = []
all_seasons = []

all_teams = []
all_dates = []

all_stadium = []
all_stadium_city = []
all_stadium_country = []


for file in all_files:
    
    if ".json" in file:
        with open(file,encoding='utf-8') as f:   
            df = json.loads(f.read())
    else:
        pass
    
    #--------------------------------------------------------------------------------------------
    # Getting officials information
    if "match_referees" in df["info"]["officials"].keys():
        for match_referees in df["info"]["officials"]["match_referees"]:
            if match_referees not in officials:
                officials.append(match_referees)
    else:
        pass
    
    if "reserve_umpires" in df["info"]["officials"].keys():
        for reserve_umpires in df["info"]["officials"]["reserve_umpires"]:
            if reserve_umpires not in officials:
                officials.append(reserve_umpires)
    else:
        pass
    
    if "tv_umpires" in df["info"]["officials"].keys():
        for tv_umpires in df["info"]["officials"]["tv_umpires"]:
            if tv_umpires not in officials:
                officials.append(tv_umpires)
    else:
        pass
        
    if "umpires" in df["info"]["officials"].keys():
        for umpires in df["info"]["officials"]["umpires"]:
            if umpires not in officials:
                officials.append(umpires)
    else:
        pass
    
    #--------------------------------------------------------------------------------------------
    # Getting Teams Information
    for key,values in df["info"]["players"].items():
        if key not in all_teams:
            all_teams.append(key)
            date = df["info"]["dates"][0]
            format_str = '%Y-%m-%d' # The format
            dto = datetime.strptime(date, format_str).date()
            all_dates.append(dto)
            
    #--------------------------------------------------------------------------------------------
    # Getting Player Information
        for player in values:
            if player not in all_players:
                all_players.append(player)
    
    #--------------------------------------------------------------------------------------------
    # Getting season Information         
    date = df["info"]["dates"][0]
    format_str = '%Y-%m-%d' # The format
    dto = datetime.strptime(date, format_str).date()
    date = dto.year
    if date not in all_seasons:
        all_seasons.append(date)
    
    #--------------------------------------------------------------------------------------------
    # Getting Stadium Information
    if df["info"]["venue"] not in all_stadium:
        all_stadium.append(df["info"]["venue"])
        try:
            all_stadium_city.append(df["info"]["city"])
        except Exception:
            all_stadium_city.append("")
            
        stadium = df["info"]["venue"]
        country = get_country(stadium)
        if country == "":
            
            
            try:
                city = df["info"]["city"]
                country = get_country(city)
                all_stadium_country.append(country)
            except Exception:
                country = ""
                all_stadium_country.append(country)
        else:
            all_stadium_country.append(country)
    
#--------------------------------------------------------------------------------------------
# Creating a Dictionary for the Data Received 
officials_data = {"official_name":officials}
players_data = {"player_name":all_players}
seasons_data = {"season_year":all_seasons}
teams_data = {"team_name":all_teams,"dates":all_dates}
stadium_data = {"stadium_name":all_stadium,"stadium_city":all_stadium_city,"stadium_country":all_stadium_country}

#--------------------------------------------------------------------------------------------
# Converting Dictionary to Pandas DataFrame
officials_df = pd.DataFrame(officials_data)
players_df = pd.DataFrame(players_data)

seasons_df = pd.DataFrame(seasons_data)
seasons_df = seasons_df.sort_values("season_year").reset_index(drop=True)

teams_df = pd.DataFrame(teams_data)
teams_df = teams_df.sort_values("dates").reset_index(drop=True)
teams_df = teams_df.drop("dates",axis = 1)

stadium_df = pd.DataFrame(stadium_data)

#--------------------------------------------------------------------------------------------
# Creating a Connection between Local Machine and The Postgres Database
engine = create_engine('postgresql://sys:Common123!@ipldb.cqhcdwcdnqbw.ap-south-1.rds.amazonaws.com:5432/postgres')

# --------------------------------------------------------------------------------------------
# Loading the Data into the Database Dimensions
players_df.to_sql('dim_players', engine,schema='ipl',if_exists = "append",index= False)
officials_df.to_sql('dim_officials', engine,schema='ipl',if_exists = "append",index= False)
seasons_df.to_sql('dim_seasons', engine,schema='ipl',if_exists = "append",index= False)
teams_df.to_sql('dim_teams', engine,schema='ipl',if_exists = "append",index= False)
stadium_df.to_sql('dim_stadium', engine,schema='ipl',if_exists = "append",index= False)

#--------------------------------------------------------------------------------------------
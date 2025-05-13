"""
builder.py
-------------
Functions to run the pipeline.

Author  : Baptiste Gorteau
Date    : May 2025
Project  : World Athletics ETL
File : Part of the ETL pipeline - step 4 (pipeline)

Main functions :
- convert_pdf_to_dict()
- create_race_db()
"""

from parser import *
from transformer import *
from downloader import *
from config import indoor_outdoor_url
import pandas as pd
import os
import logging
import warnings

warnings.filterwarnings("ignore")

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#===================================================================#
#======================= PIPELINE FUNCTIONS ========================#
#===================================================================#

#========== Convert the PDF result book into a dictionary ==========#

def convert_pdf_to_dict(file_path):
  # Open and merge result book file's data
  race_rb_data = open_and_merge_pdf_data(file_path)

  # Separate race's description and times data
  race_description_and_times = get_race_description_and_times(race_rb_data)
  race_description = race_description_and_times["race_description"]
  race_times = race_description_and_times["race_times"]

  #======= Get race's information =======
  # Get race's distance
  race_distance = get_race_dist(race_description)

  # Get race's location
  race_location = get_race_location(race_description)

  # Get race's date, temperature and humidity
  race_d_t_h = get_race_date_temperature_humidity(race_description)
  race_date = race_d_t_h["race_date"]
  race_temperature = race_d_t_h["race_temperature"]
  race_humidity = race_d_t_h["race_humidity"]

  # Dictionary for race's information
  race_info = {"distance":race_distance,
                "location": race_location,
                "date": race_date,
                "temperature": race_temperature,
                "humidity": race_humidity}

  #======= Get race's splits =======
  # Get first time splits' dictionary
  first_ts_dict = get_first_time_splits_dictionary(race_times)

  # Initialize a new dictionnary for clean data
  new_dict_times = {}
  # Initialize a dictionary for abandoned data
  abandoned_data = {}

  # Iterate on 'first_ts_dict'
  for key, val in first_ts_dict.items():
    # Get athlete's first and last name
    athlete_name = get_athlete_first_and_last_name(key)

    # Get athlete's country and age
    athlete_country_and_age = get_athlete_country_and_age(key, race_date)
    athlete_country = athlete_country_and_age["athlete_country"]
    athlete_age = athlete_country_and_age["athlete_age"]

    # Check split's incoherences
    incoherent_splits = check_splits_incoherences(val, race_distance)

    # Add the data to 'new_dict_times'
    if (
        (type(val) == list) and
         (len(val) == (race_distance/100)) and
          (incoherent_splits == False)):

      # Add info to the dictionary
      new_dict_times[athlete_name] = {}
      # Athlete info
      new_dict_times[athlete_name]["athlete_info"] = {"name": athlete_name,
                                                      "country": athlete_country,
                                                      "age": athlete_age}
      # Athlete time splits
      new_dict_times[athlete_name]["time_splits"] = val

    # Keep track of abandoned data
    is_abandoned = is_abandoned_data(val, key, race_distance, incoherent_splits,
                                     athlete_name)
    abandoned = is_abandoned["abandoned"]
    abandon_reason = is_abandoned["reason"]

    if abandoned == True:
      abandoned_data[athlete_name] = {}
      abandoned_data[athlete_name]["athlete_info"] = {"name": athlete_name,
                                                      "country": athlete_country,
                                                      "age": athlete_age}

      abandoned_data[athlete_name]["reason"] = abandon_reason

  # Create final dictionary for output
  final_dict = {"race_info":race_info, "athletes":new_dict_times, "abandoned_data":abandoned_data}

  # Return
  return final_dict

#========== Create a dataframe for each event ==========#

def create_race_db(results_books_pdf, rb_pdf_link):
  # Create a dictionary of dataframes for each event
  event_df_dict = {'800-metres': pd.DataFrame(),
                 '1500-metres': pd.DataFrame(),
                 '3000-metres': pd.DataFrame(),
                 '3000-metres-steeplechase': pd.DataFrame(),
                 '5000-metres': pd.DataFrame(),
                 '10000-metres': pd.DataFrame(),
                 'abandoned-data': pd.DataFrame()}

  # Set the index for abandoned data ('ad_ind') to 0
  ad_ind = 0

  for comp_type, list_comp in results_books_pdf.items():
    for comp_year, events in list_comp.items():
      for event, pdf_links in events.items():
        # Get the info about the race
        race_info = event.split(" ")
        m_w = race_info[0]
        race_name = race_info[1]
        race_stage = race_info[2]
        if race_name in list(event_df_dict.keys()) and len(pdf_links) > 0:
          for race_nb in range(len(pdf_links)):
            try :
              # Download the pdf
              download_pdf(pdf_links[race_nb], rb_pdf_link)
              # Read the info of the pdf
              race_res_from_pdf = convert_pdf_to_dict(rb_pdf_link)
              # Delete the pdf
              os.remove(rb_pdf_link)

              #======= Transform the dictionary into a dataframe =======#
              ind = 0
              race_df = pd.DataFrame()
              for key, val in race_res_from_pdf["athletes"].items():
                race_df.loc[ind, "COMP_TYPE"] = comp_type
                race_df.loc[ind, "COMP_LOC"] = race_res_from_pdf["race_info"]["location"]
                race_df.loc[ind, "YEAR"] = comp_year
                race_df.loc[ind, "RACE_DATE"] = race_res_from_pdf["race_info"]["date"]
                race_df.loc[ind, "RACE_TEMP"] = race_res_from_pdf["race_info"]["temperature"]
                race_df.loc[ind, "RACE_HUMID"] = race_res_from_pdf["race_info"]["humidity"]
                race_df.loc[ind, "M_W"] = m_w
                race_df.loc[ind, "EVENT"] = race_name
                race_df.loc[ind, "STAGE"] = race_stage
                race_df.loc[ind, "RACE_NB"] = race_nb+1
                race_df.loc[ind, "ATHLETE_NAME"] = val["athlete_info"]["name"]
                race_df.loc[ind, "ATHLETE_COUNTRY"] = val["athlete_info"]["country"]
                race_df.loc[ind, "ATHLETE_AGE"] = val["athlete_info"]["age"]
                for i in range(len(val['time_splits'])):
                  race_df.loc[ind, '{}M'.format((i+1)*100)] = val['time_splits'][i]
                # Update the index
                ind += 1
              event_df_dict[race_name] = pd.concat([event_df_dict[race_name], race_df]).reset_index(drop=True)

              #======= Transform the dictionary into a dataframe =======#

              for key, val in race_res_from_pdf["abandoned_data"].items():
                event_df_dict['abandoned-data'].loc[ad_ind, "COMP_TYPE"] = comp_type
                event_df_dict['abandoned-data'].loc[ad_ind, "COMP_LOC"] = race_res_from_pdf["race_info"]["location"]
                event_df_dict['abandoned-data'].loc[ad_ind, "YEAR"] = comp_year
                event_df_dict['abandoned-data'].loc[ad_ind, "RACE_DATE"] = race_res_from_pdf["race_info"]["date"]
                event_df_dict['abandoned-data'].loc[ad_ind, "RACE_TEMP"] = race_res_from_pdf["race_info"]["temperature"]
                event_df_dict['abandoned-data'].loc[ad_ind, "RACE_HUMID"] = race_res_from_pdf["race_info"]["humidity"]
                event_df_dict['abandoned-data'].loc[ad_ind, "M_W"] = m_w
                event_df_dict['abandoned-data'].loc[ad_ind, "EVENT"] = race_name
                event_df_dict['abandoned-data'].loc[ad_ind, "STAGE"] = race_stage
                event_df_dict['abandoned-data'].loc[ad_ind, "RACE_NB"] = race_nb+1
                event_df_dict['abandoned-data'].loc[ad_ind, "ATHLETE_NAME"] = val["athlete_info"]["name"]
                event_df_dict['abandoned-data'].loc[ad_ind, "ATHLETE_COUNTRY"] = val["athlete_info"]["country"]
                event_df_dict['abandoned-data'].loc[ad_ind, "ATHLETE_AGE"] = val["athlete_info"]["age"]
                event_df_dict['abandoned-data'].loc[ad_ind, "REASON"] = val['reason']
                ad_ind += 1
              #======= Add info about the treated race =======#
              logging.info(f"{event} {race_stage} number {race_nb} from {comp_year} {comp_type} treated.")
            except Exception as e:
              print(f"Error when scrapping {race_info} nb {race_nb+1} from {comp_year} {comp_type}")
  return event_df_dict

#===================================================================#
#============================ PIPELINE =============================#
#===================================================================#

#========== Get the list of world championship competitions ==========#
list_wc = get_competitions_links(indoor_outdoor_url)

#========== Create a dictionary of all results books' PDF links ==========#
results_books_pdf = {}
for comp_type, comp_links in list_wc.items():
  # Add the competition type (indoor/outdoor) to the dictionary
  results_books_pdf[comp_type] = {}
  # Find the events for a competition
  for comp in comp_links:
    # Add the year of the competition to the dictionary
    comp_year = comp.split("/")[5]
    if int(comp_year) >= 2019:
      results_books_pdf[comp_type][comp_year] = {}
      # Retrieve the events for a competition
      comp_events = get_races_url(comp)
      # Add the pdf links of the competition's events
      for event_name, event_url in comp_events.items():
        results_books_pdf[comp_type][comp_year][event_name] = access_pdf(event_url)

#========== Create a dataframe for each event ==========#
wa_data = create_race_db(results_books_pdf, 'data/rb_storage/result_book.pdf')

#========== Display a message if the data has been retrieved ==========#
logging.info("World Athletics data retrieved and transformed.")

#========== Reasons for abandonned data ==========#
abandoned_data = wa_data["abandoned-data"]
for i in pd.unique(wa_data["abandoned-data"]["REASON"]):
  print(i, ":", len(wa_data["abandoned-data"][wa_data["abandoned-data"]["REASON"] == i]))

#========== Percentage of the data kept ==========#
nb_rows = 0
for key, val in wa_data.items():
  if key != "abandoned-data":
    nb_rows += len(val)
print(f"{round((nb_rows/(nb_rows+len(abandoned_data)))*100,2)}% of the performance was kept.")

#========== Save the data ==========#
for key, val in wa_data.items():
  directory = "data/races"
  filepath = f"{directory}/{key}.csv"
  # Save the race data
  val.to_csv(filepath, index=False)

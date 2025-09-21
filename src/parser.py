"""
parser.py
-------------
Functions for parsing information from result books.

Author  : Baptiste Gorteau
Date    : May 2025
Project  : World Athletics ETL
File : Part of the ETL pipeline - step 2 (parsing)

Main functions :
- open_and_merge_pdf_data()
- get_race_description_and_times()
- get_race_dist()
- get_race_location()
- get_race_date_temperature_humidity()
- get_first_time_splits_dictionary()
- get_athlete_first_and_last_name()
- get_athlete_country_and_age()
"""

import PyPDF2
import re
import regex
from datetime import datetime, timedelta
from config import mid_dist_and_dist_races

#========== Read the PDF data and merge all the pages ==========#

def open_and_merge_pdf_data(file_path):
  with open(file_path, "rb") as pdf_file:
    # Read the PDF file of the result book
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    # We initialize the variable which will contains all the pages
    all_pages = ''
    # We iterate on the pages
    for num_page in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[num_page]
        # We extract the text from the page
        page_text = page.extract_text()
        # We add the text form the page to the 'all_pages' variable
        all_pages += page_text
  # Return
  return all_pages

#========== Separate the race description from the race splits ==========#

def get_race_description_and_times(race_rb_data):
  # We search for the ' m ' character
  number_lines_w_m = len(
      [line for line in race_rb_data.split("\n") if ' m ' in line]
      )

  # Intialize the list which will contains race description lines
  race_description = []
  # Intialize the list which will contains the race splits
  race_times = []
  # Set the ' m ' counter to 0
  cpt_m = 0
  # Iterate on the document's lines
  for line in race_rb_data.split("\n") :
      # if the ' m ' counter bellow 'number_lines_w_m' we add the info to 'race_description'
      if cpt_m < number_lines_w_m :
          race_description.append(line)
      # Else, we add the line to 'race_times'
      else :
          race_times.append(line)
      # We update the 'cpt_m' value if ' m ' is found in the line
      if ' m ' in line :
          cpt_m += 1
  # Return
  return {"race_description": race_description, "race_times": race_times}

#========== Get race distance ==========#

def get_race_dist(race_description):
  # We set the race distance variable as an empty string
  race_dist = ""
  # We iterate over the values of the
  for i in race_description :
    for j in i.split(" "):
      if ((j.replace(",", "").replace(".", "").replace("m", "") in mid_dist_and_dist_races) and (i.split(" ") != 'm')):
        race_dist = int(j.replace(",", "").replace(".", "").replace("m", ""))
        break
    if race_dist != "":
      break
  return race_dist

#========= Get race location ==========#

def get_race_location(race_description):
  # We retrieve the line which contains the race's location and we split it
  race_loc_list = race_description[2].rsplit(" ")
  # We set up the variable which will contain the index of the location's country
  country_loc_in_list = None
  # We iterate on the splited line and we look for the last string with an openning comma
  for i in range(len(race_loc_list)):
    if "(" in race_loc_list[i]:
      country_loc_in_list = i
  # We create the 'race_location' variable if the index is found
  if country_loc_in_list != None:
    # We merge location's city and country together
    race_location = ' '.join(race_loc_list[country_loc_in_list-1:country_loc_in_list+1])
    if race_location.split(" ")[-1] == '':
      # If no country is found, we only keep the city for the location
      race_location = race_location.rsplit(" ", 1)[0]
  # If no location is found, we use an empty string
  else:
    race_location = ""
  # Return
  return race_location

#========== Get race date, temperature and humidity ==========#

def get_race_date_temperature_humidity(race_description):
  # Get the string with race date, temperature and humidity
  race_spec_info = [i for i in race_description if (len(i) >= 3 and i[0] == ' ' and i[1].isdigit())][0]

  # Race date
  race_date = ' '.join([i for i in race_spec_info.split(" ") if i != ''][:3])

  # Race temperature
  race_temp_in_list = [i for i in race_spec_info.split(" ") if "°" in i]
  # We check if we have the temperature in the 'race_temp_in_list' list
  if len(race_temp_in_list) > 0:
    race_temp = race_temp_in_list[0].split("°")[0]
  # If no element in 'race_temp_in_list', no temperature and empty string
  else:
    race_temp = ''

  # Race humidity
  race_humidity_list = race_spec_info.split(" %")
  if len(race_humidity_list) >= 1:
    race_humidity = race_humidity_list[0].split(" ")[-1]
  else:
    race_humidity = ""
  # Return
  return {"race_date":race_date,
          "race_temperature":race_temp,
          "race_humidity":race_humidity}

#========== Create a first dictionary with the times ==========#

def get_first_time_splits_dictionary(race_times):
  # Set up a dictionary that will receive the values
  dico_times = {}
  # Set up the current key with an empty string
  curent_key = ''
  # We iterate on the 'race_times'
  for i in race_times:
      # '  ' respresents the double space between the first and last name of the athlete
      # The 'SEIKO' brand name can sometimes be present near the time of the athlete
      if '  ' in i and 'SEIKO' not in i :
        # If the conditions are respected, the iterated value is considered as the current key
        curent_key = i
        # We set up an empty list as the value corresponding to the key
        dico_times[curent_key] = []
      # The comas symbols are present near the times to specify the position at the split
      if '(' in i and ')' in i :
        # If the condition is respected, we add the time to the list
        dico_times[curent_key].append(i.split(" ")[0])

  # Add the final time to the time dictionary
  for key, val in dico_times.items():
      if ("DQ" not in key.split(" ")) and ("DNF" not in key.split(" ")):
        final_time = ''
        for i in key.split(" "):
            # Manage the disqualifications
            if ':' in i and '.' in i:
                final_time = i
        val.append(final_time)
      else:
        if "DQ" in key.split(" "):
          dico_times[key] = "DQ"
        if "DNF" in key.split(" "):
          dico_times[key] = "DNF"
  # Return
  return dico_times

#========== Get athlete first and last name ==========#

def get_athlete_first_and_last_name(dico_key):
  # Get athlete first name
  athlete_first_name = [w for w in dico_key.split("  ")[0].split(" ") if (
      (bool(re.search(r"[a-z]", w))==True) and
        (bool(regex.search(r'\p{Lu}', w))==True))]

  # get athlete last name
  athlete_last_name = [w for w in dico_key.split("  ")[1].split(" ") if (
      (w.isupper()) and
        (w != "DQ") and
        (w != "DNF") and
          bool(re.search(r"\d", w)) == False)]

  # Combine athlete's first and last name
  if len(athlete_last_name) > 1:
    athlete_last_name = athlete_last_name[:len(athlete_last_name)-1]
  athlete_name = " ".join(athlete_first_name) + " " + " ".join(athlete_last_name)

  # Return
  return athlete_name

#========== Get the athlete's country and age ==========#

def get_athlete_country_and_age(dico_key, race_date):
  # Get the splitted key
  splitted_key = dico_key.split(" ")

  # We check if the last character of the splitted key is ''
  if splitted_key[-1] == '':
    splitted_key = splitted_key[:len(splitted_key)-1]

  # Get athlete country
  athlete_country = splitted_key[len(splitted_key)-1]

  # Get athlete DOB
  athlete_DOB = ' '.join(splitted_key[len(splitted_key)-4:len(splitted_key)-1])

  # Calculate athelte age at the time of the race
  # Convert the race date in datetime format
  race_date_datetime = datetime.strptime(race_date, "%d %B %Y")
  # Convert the date of birth in datetime format
  try:
    DOB_datetime = datetime.strptime(athlete_DOB, "%d %b %Y")
  except ValueError:
        try:
          DOB_datetime = datetime.strptime(athlete_DOB, "%d %b %y")
        except ValueError:
            DOB_datetime = ''

  # Calculate the age from the two dates
  if DOB_datetime != '':
    diff_in_days = abs((race_date_datetime - DOB_datetime).days)
    athlete_age = round(diff_in_days / 365.25,0)
  else :
    athlete_age = ''
  # Return
  return {"athlete_country": athlete_country,
          "athlete_age": athlete_age}

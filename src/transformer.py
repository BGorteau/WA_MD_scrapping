"""
transformer.py
-------------
Functions to transform the parsed informations from the result book.

Author  : Baptiste Gorteau
Date    : May 2025
Project  : World Athletics ETL
File : Part of the ETL pipeline - step 3 (transformation)

Main functions :
- convert_time_string_to_numeric_seconds()
- check_extreme_values_by_100m()
- check_splits_incoherences()
- is_abandoned_data()
"""

#========= Convert time format from string to numeric =========

def convert_time_string_to_numeric_seconds(time_string):
  minute = 0
  if ":" in time_string :
    minute = float(time_string.split(":")[0])
    seconds = float(time_string.split(":")[1])
  else :
    seconds = float(time_string)
  return round((minute*60) + seconds,2)

#========= Check if there are extreme values in time splits =========#

def check_extreme_values_by_100m(values):
  # Set up the list of times for each 100m
  val_by_100 = []
  # We add the first 100m to the list
  val_by_100.append(values[0])
  # We compute the other times at each 100m
  for i in range(1, len(values)):
    val_by_100.append(values[i]-values[i-1])
  # We set the 'is_pb' variable to false
  is_pb = False
  # We check if
  for i in val_by_100:
    if len([v for v in val_by_100 if abs(v-i) >= 10]) != 0:
      is_pb = True

  return(is_pb)

#========= Check if there are incoherences in the time splits =========#

def check_splits_incoherences(dico_value, race_dist):
  # Set up the list with the numeric times in seconds
  numeric_splits = []
  # We set the 'incoherent_splits' variable to True
  incoherent_splits = True
  # We check if the value of the dictionary ids a list and have the right number of splits
  if type(dico_value) == list and (len(dico_value)==(race_dist/100)):
    numeric_splits = [convert_time_string_to_numeric_seconds(v) for
                      v in dico_value if
                       (type(v) == str and ((":" in v) or ("." in v)))]
    # We check again if the number of splits is right
    if len(numeric_splits)==(race_dist/100):
      incoherent_splits = check_extreme_values_by_100m(numeric_splits)
  # Return
  return incoherent_splits

#========= Keep track of abandonned data =========#

def is_abandoned_data(dict_value, dict_key, race_dist, incoherent_splits,
                      athlete_name):
  # Initialize 'is_abandoned' to False
  is_abandoned = False
  # Initialize 'abandon_reason' to ''
  abandon_reason = ''

  # Add info about the abandoned data
  if (((dict_value == "DQ") or # We add the athlete if he was DQ
        (dict_value == "DNF") or # We add the athlete if he DNF
        ((type(dict_value) == list) and (len(dict_value) != (race_dist/100))) or # We add the athlete of the time splits are incomplete
          (incoherent_splits == True)) and # We add the athlete if the time splits are incoherent
        (athlete_name not in [' ', '']) and  # We make sure that the athlete exist
        ('BIB' not in dict_key.split(" "))): # We make sure that the athlete wasn't caught in the footpage
    is_abandoned = True

    # Reason why the data was abandoned
    if dict_value == "DQ":
      abandon_reason = "Disqualification"
    else:
      if dict_value == "DNF":
        abandon_reason = "Did not finish"
      else:
        if ((type(dict_value) == list) and (len(dict_value) != (race_dist/100))):
          abandon_reason = "Missing time split"
        else:
          if incoherent_splits == True:
            abandon_reason = "Incoherent splits"
          else:
            abandon_reason = "Unknown"

  # Return
  return {"abandoned": is_abandoned, "reason": abandon_reason}

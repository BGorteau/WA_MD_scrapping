"""
analyze.py
-------------
Analyze world athletics data created with the pipeline.

Author  : Baptiste Gorteau
Date    : May 2025
Project  : World Athletics ETL
File : World Athletics championships data analysis.

Main functions :
- convert_time_string_to_numeric_seconds()
- hundred_m_times()
- s_m_to_km_h()
- mean_sd_speed()
- smooth_values()
- plot_race_speed()
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib import lines, patches
from scipy.interpolate import make_interp_spline

#===================================================================#
#=========================== IMPORT DATA ===========================#
#===================================================================#

data_1500m = pd.read_csv("data/races/1500-metres.csv")
data_800m = pd.read_csv("data/races/800-metres.csv")

#===================================================================#
#============================ FUNCTIONS ============================#
#===================================================================#

def convert_time_string_to_numeric_seconds(time_string):
  minute = 0
  # We force the string type
  time_string = str(time_string)
  if ":" in time_string :
    minute = float(time_string.split(":")[0])
    seconds = float(time_string.split(":")[1])
  else :
    seconds = float(time_string)
  return round((minute*60) + seconds,2)

# Get the times in seconds at each 100m
def hundred_m_times(list_seconds) :
    new_list = [float(list_seconds[0])]
    for i in range(1,len(list_seconds)) :
        new_list.append(float(round(list_seconds[i]-list_seconds[i-1], 2)))
    return new_list

# Convert a time in seconds per 100m to km/h
def s_m_to_km_h(sec) :
    return round((0.1*3600)/sec,2)

# Create a dataframe with the speed in km/h at each 100m
def df_kmh_per_100m(data) :
  # Get the distance splits columns
  split_cols = [c for c in data.columns if c not in ['COMP_TYPE', 'COMP_LOC',
       'YEAR', 'RACE_DATE', 'RACE_TEMP', 'RACE_HUMID', 'M_W',
       'EVENT', 'STAGE', 'RACE_NB', 'ATHLETE_NAME', 'ATHLETE_COUNTRY',
       'ATHLETE_AGE']]
  # We duplicate thw inpiut dataframe in a new variable
  new_df = data.copy()
  # We iterate on a each performance of the duplicated dataframe
  for i in range(len(new_df)):
    # We retreive the times of the race in a list
    times = list(new_df.loc[i, split_cols])
    # We convert the times in seconds
    times_in_sec = [convert_time_string_to_numeric_seconds(i) for i in times]
    # We get the times for each 100m
    sec_hundred_meters = hundred_m_times(times_in_sec)
    # We get the speed for each 100m
    kmh_per_hundred = [s_m_to_km_h(i) for i in sec_hundred_meters]
    # We add the speeds to the duplicated dataframe
    new_df.loc[i, split_cols] = kmh_per_hundred
  # Return
  return new_df

# Get the mean and standard deviation of the speed for each 100m
def mean_sd_speed(df) :
    # We only keep the columns of the input dataframe that are distance split
    df_plot = df[[c for c in df.columns if c not in ['COMP_TYPE', 'COMP_LOC', 'YEAR',
       'RACE_DATE', 'RACE_TEMP', 'RACE_HUMID', 'M_W', 'EVENT', 'STAGE', 'RACE_NB',
       'ATHLETE_NAME', 'ATHLETE_COUNTRY','ATHLETE_AGE']]]
    # We create a dictionary with the mean, upper and lower standard deviations of the speed at each 100m
    data = {"mean" : [np.mean(df_plot[col]) for col in df_plot.columns],
            "sd_plus" : [np.mean(df_plot[col]) + np.std(df_plot[col]) for col in df_plot.columns],
            "sd_moins" : [np.mean(df_plot[col]) - np.std(df_plot[col]) for col in df_plot.columns]}
    # We convert this dictionary into a Pandas dataframe
    data_pd = pd.DataFrame.from_dict(data, orient="index", columns=df_plot.columns)
    # Return
    return data_pd

# Smooth values with spline basis
def smooth_values(value_list, nb_points=150):
  x = np.arange(len(value_list))
  # Create a spline interpolation
  x_smooth = np.linspace(x.min(), x.max(), nb_points)
  spline = make_interp_spline(x, value_list, k=3)  # k=3 cubic spline
  y_smooth = spline(x_smooth)
  return y_smooth

# Plot race speed
def plot_race_speed(ax, list_1, list_2, race_length, is_legend=False):
  # Plot the two lists
  ax.plot(smooth_values(list_1, int(race_length/10)), color="red", label="indoor")
  ax.plot(smooth_values(list_2, int(race_length/10)), color="blue", label="outdoor")
  # Remove axis and background

  # Set the x ticks and labels
  x_ticks = np.linspace(5, int(race_length/10)+5, int(race_length/100))
  x_ticks_labels = [f"{i}m" for i in range(100,int(race_length)+100, 100)]
  ax.set_xticks(x_ticks, x_ticks_labels, rotation=45)
  ax.set_axisbelow(True)
  ax.grid(axis = "x", color="#A8BAC4", lw=1.2)
  ax.spines["right"].set_visible(False)
  ax.spines["top"].set_visible(False)
  ax.spines["bottom"].set_visible(False)
  ax.spines["left"].set_lw(1.5)

  ax.spines["left"].set_capstyle("butt")

  ax.set_facecolor('none')


  ax.set_ylabel("Spped (km/h)", fontfamily="DejaVu Sans")

  ax.tick_params(axis='x', which='both', length=0)

  # Legend
  if is_legend == True:
    ax.legend(loc='lower center', fontsize=20, frameon=False, bbox_to_anchor=(0.15, -0.35))

#===================================================================#
#=============== CREATE DATAFRAMES FOR THE ANALYSIS ================#
#===================================================================#

#========== 1500m ==========
data_1500m_kmh = df_kmh_per_100m(data_1500m)

# 1500m men's indoor
data_1500_men_indoor = data_1500m_kmh[
    (data_1500m_kmh["COMP_TYPE"] == "indoor") &
     (data_1500m_kmh["M_W"] == "men")]

data_1500_men_indoor_mean_sd = mean_sd_speed(data_1500_men_indoor)

# 1500m men's outdoor
data_1500_men_outdoor = data_1500m_kmh[
    (data_1500m_kmh["COMP_TYPE"] == "outdoor") &
     (data_1500m_kmh["M_W"] == "men")]

data_1500_men_outdoor_mean_sd = mean_sd_speed(data_1500_men_outdoor)

# 1500m women's indoor
data_1500_women_indoor = data_1500m_kmh[
    (data_1500m_kmh["COMP_TYPE"] == "indoor") &
     (data_1500m_kmh["M_W"] == "women")]

data_1500_women_indoor_mean_sd = mean_sd_speed(data_1500_women_indoor)

# 1500m women's outdoor
data_1500_women_outdoor = data_1500m_kmh[
    (data_1500m_kmh["COMP_TYPE"] == "outdoor") &
     (data_1500m_kmh["M_W"] == "women")]

data_1500_women_outdoor_mean_sd = mean_sd_speed(data_1500_women_outdoor)


#========== 800m ==========
data_800m_kmh = df_kmh_per_100m(data_800m)

# 800m men's indoor
data_800_men_indoor = data_800m_kmh[
    (data_800m_kmh["COMP_TYPE"] == "indoor") &
     (data_800m_kmh["M_W"] == "men")]

data_800_men_indoor_mean_sd = mean_sd_speed(data_800_men_indoor)

# 800m men's outdoor
data_800_men_outdoor = data_800m_kmh[
    (data_800m_kmh["COMP_TYPE"] == "outdoor") &
     (data_800m_kmh["M_W"] == "men")]

data_800_men_outdoor_mean_sd = mean_sd_speed(data_800_men_outdoor)

# 800m women's indoor
data_800_women_indoor = data_800m_kmh[
    (data_800m_kmh["COMP_TYPE"] == "indoor") &
     (data_800m_kmh["M_W"] == "women")]

data_800_women_indoor_mean_sd = mean_sd_speed(data_800_women_indoor)

# 800m women's outdoor
data_800_women_outdoor = data_800m_kmh[
    (data_800m_kmh["COMP_TYPE"] == "outdoor") &
     (data_800m_kmh["M_W"] == "women")]

data_800_women_outdoor_mean_sd = mean_sd_speed(data_800_women_outdoor)

#===================================================================#
#========================= ANALYSIS (PLOT) =========================#
#===================================================================#

fig, axs = plt.subplots(2, 2, figsize=(15, 15))

# Legend

plot_race_speed(axs[0,0], list(data_1500_men_indoor_mean_sd.loc["mean"]),
                list(data_1500_men_outdoor_mean_sd.loc["mean"]), 1500)

plot_race_speed(axs[1,0], list(data_1500_women_indoor_mean_sd.loc["mean"]),
                list(data_1500_women_outdoor_mean_sd.loc["mean"]), 1500, 
                is_legend=True)

plot_race_speed(axs[0,1], list(data_800_men_indoor_mean_sd.loc["mean"]),
                list(data_800_men_outdoor_mean_sd.loc["mean"]), 800)

plot_race_speed(axs[1,1], list(data_800_women_indoor_mean_sd.loc["mean"]),
                list(data_800_women_outdoor_mean_sd.loc["mean"]), 800)

# Add other elements

# plt.legend(loc='lower right', fontsize=15, frameon=False, bbox_to_anchor=(1, -0.25))

# Make room on top and bottom
fig.subplots_adjust(left=0.020, right=1, top=0.5, bottom=0.05)
plt.tight_layout(rect=[0.02, 0.1, 0.98, 0.87])

# Add title
fig.text(
    0, 0.95, "Evolution of average speed over 800m and 1500m races during the world championships",
    fontsize=20, fontweight="bold", fontfamily="DejaVu Sans"
)
# Add subtitle
fig.text(
    0, 0.92, "World Athletics Championships races data from 2019 to 2025.",
    fontsize=16, fontfamily="DejaVu Sans"
)

# Add caption
source = "Source: World Athetics data (worldathletics.org/competition/calendar-results)"
fig.text(
    0, 0.05, source, color="#a2a2a2",
    fontsize=14, fontfamily="DejaVu Sans"
)

# Add authorship
fig.text(
    0, 0.03, "Baptiste Gorteau - bgorteau.github.io", color="#a2a2a2",
    fontsize=16, fontfamily="DejaVu Sans"
)

fig.text(-0.005, 0.67, 'Men', va='center', rotation='vertical', fontsize=20, fontweight="bold", fontfamily="DejaVu Sans")
fig.text(-0.005, 0.28, 'Women', va='center', rotation='vertical', fontsize=20, fontweight="bold", fontfamily="DejaVu Sans")

fig.text(0.25, 0.88, '1500m', va='center', fontsize=20, fontweight="bold", fontfamily="DejaVu Sans")
fig.text(0.75, 0.88, '800m', va='center', fontsize=20, fontweight="bold", fontfamily="DejaVu Sans")

# Add line and rectangle on top.
fig.add_artist(lines.Line2D([0, 1], [1, 1], lw=3, color="#E3120B", solid_capstyle="butt"))
fig.add_artist(patches.Rectangle((0, 0.975), 0.05, 0.025, color="#E3120B"))

# Set facecolor, useful when saving as.png
fig.set_facecolor("white")
plt.savefig("figures/1500_800_analysis.png", bbox_inches='tight', dpi=300)

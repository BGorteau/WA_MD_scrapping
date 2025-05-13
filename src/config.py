"""
config.py
-------------
Main project variables

Author  : Baptiste Gorteau
Date    : May 2025
Project  : World Athletics ETL
File : File containing the project variables
"""

# Indoor and outdoor competition pages url
indoor_outdoor_url = {
    "outdoor":"https://worldathletics.org/results/world-athletics-championships",
    "indoor":"https://worldathletics.org/results/world-athletics-indoor-championships"
    }

# List of middle-distance and distance events in World Athletics competitions
distance_events = ['1500-metres', '3000-metres-steeplechase', '10000-metres',
                   '3000-metres', '5000-metres', '800-metres']

# Distances of middle-distance and distance races
mid_dist_and_dist_races = ["800", "1500", "3000", "3000-steeplechase", "5000",
                           "10000"]

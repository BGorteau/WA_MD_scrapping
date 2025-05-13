"""
downloader.py
-------------
Functions to get the results links in PDF from the World Athletics website and then download them.

Author  : Baptiste Gorteau
Date    : May 2025
Project  : World Athletics ETL
File : Part of the ETL pipeline - step 1 (extraction)

Main functions :
- get_competitions_links()
- get_races_url()
- download_pdf()
"""

from config import distance_events
import requests
from bs4 import BeautifulSoup

#========== Get competitions links ==========#

def get_competitions_links(comp_pages_dict):
    # We set up the dictionary that will host the links of the competitions
    list_wc = {}
    for comp_type, url in comp_pages_dict.items():
        list_wc[comp_type] = []
        # We set the 'verifiy' parameter to False to avoid the SSl verification
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            # Find all links ('a' tags) on the page
            links = soup.find_all("a")

            # Browse links and look for those pointing to PDF files
            for link in links:
                link_href = link.get("href")
                # retrieve the year of the competition
                if (
                    (('/results/world-athletics-championships' in str(link_href)) or
                    ('/results/world-athletics-indoor-championships' in str(link_href))) and
                        ('http' not in str(link_href))
                        ) :
                    if int(link_href.split("/")[3]) >= 2019:
                        list_wc[comp_type].append('https://worldathletics.org'+str(link_href))
    # Return
    return list_wc

#=========== Retrieve the links of the events for a competition ===========#

def get_races_url(url):
    # Acces all races links of an event
    response = requests.get(url, verify=False)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch URL: {url}")
    all_links = {}
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all("a")
    for link in links :
        link_href = link.get("href")
        link_href_split = str(link_href).split("/")
        # Check if the lenght from the split of the link is superior to 6
        if (len(link_href_split) > 6) :
            # We apply conditions to select the right links
            if ((link_href_split[6] in distance_events) and
             ('result#resultheader' in link_href_split[8])):
              # We add the competition's links to the dictionary
              all_links['{} {} {}'.format(link_href_split[5],
                                          link_href_split[6],
                                          link_href_split[7])
              ] = 'https://worldathletics.org'+str(link_href)
    # Return
    return all_links

#========== Access the results books links for a race ==========#

def access_pdf(url) :
  response = requests.get(url, verify=False)
  if response.status_code != 200:
    raise ValueError(f"Failed to fetch URL: {url}")
  # List that will contains results books' links
  pdf_links = []
  soup = BeautifulSoup(response.content, "html.parser")

  # The elements that contains the results books' links are inside tags with the class name 'seiko'
  seiko_elements = soup.find_all(class_='seiko')

  # We iterate on each 'seiko' element
  for element in seiko_elements :
      # We retrieve the '<a>' tags from each element
      links = element.find_all("a", href=True)
      for link in links :
          # If the second element from the tag is 'Download' we keep the tag
          if 'Download' in list(link)[2] :
              # Keep the link related to the tag
              link_href = link["href"]
              # We make sure that the link does not corresponds to the finish photo
              if 'photo' not in str(link_href) :
                  # We add the link of the result book to the 'pdf_links' list
                  pdf_links.append('https://worldathletics.org'+str(link_href))
  # Return
  return pdf_links

#========== Download the pdf from its link ==========#

def download_pdf(url, storage_link) :

    response = requests.get(url, verify=False)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch URL: {url}")

    with open(storage_link, "wb") as fichier:
        fichier.write(response.content)

from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import re
import os

# Define headers to avoid request blocking
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win 64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36"
}

# Initialize variables
cities = []
base_url = "https://www.magicbricks.com/property-for-sale-rent-in-Pune/residential-real-estate-Pune"
html_text = requests.get(base_url, headers=headers).text
soup = BeautifulSoup(html_text, "lxml")

# Extract city names
container = soup.find("div", class_='city-drop-lt')
if container:
    cities = [item.text.strip() for item in container.find_all('li')]

# Load existing data to avoid duplicates
city_csv_path = "scraped_house_data.csv"
existing_data = pd.read_csv(city_csv_path) if os.path.isfile(city_csv_path) else pd.DataFrame()
processed_cities = existing_data['City'].unique().tolist() if not existing_data.empty else []

print(f"Already processed cities: {processed_cities}")

# Function to fetch data from a single page
def fetch_page_data(city_slug, page_num):
    url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs/page-{page_num}"
    properties_data = []

    try:
        page_html_text = requests.get(url, headers=headers).text
        page_soup = BeautifulSoup(page_html_text, "lxml")
        properties_container = page_soup.find_all('div', class_='mb-srp__card')

        for property_card in properties_container:
            # Extract basic property details
            title_tag = property_card.find('h2', class_='mb-srp__card--title')
            title = title_tag.text.strip() if title_tag else "No Title"
            amount = property_card.find("div", class_='mb-srp__card__price--amount')
            amount = amount.text.strip() if amount else None
            price_per_sqft = property_card.find("div", class_='mb-srp__card__price--size')
            price_per_sqft = price_per_sqft.text.strip() if price_per_sqft else None
            description = property_card.find("div", class_='mb-srp__card--desc--text')
            description = description.text.strip() if description else None

            # Extract details URL from JSON data
            script_tag = property_card.find('script', type="application/ld+json")
            json_data = json.loads(script_tag.string) if script_tag else {}
            details_url = json_data.get("url")

            # Initialize detailed attributes
            ageOfConstruction = carpet_area = status = floor = transaction = furnishing = None
            facing = overlooking = society = bathroom = balcony = car_parking = ownership = None
            super_area = dimensions = plot_area = posted_date = None

            # Fetch additional details from the property details page
            if details_url:
                try:
                    detail_html_text = requests.get(details_url, headers=headers).text
                    detail_soup = BeautifulSoup(detail_html_text, 'lxml')
                    posted_date_tag = detail_soup.find('span', class_="mb-ldp__posted--date")
                    posted_date = posted_date_tag.text.strip() if posted_date_tag else "No Posted Date"

                    # Fetch 'More Details' section
                    details_container = detail_soup.find('section', id="more-details")
                    if details_container:
                        li_tags = details_container.find_all('li', class_="mb-ldp__more-dtl__list--item")
                        for li in li_tags:
                            label = li.find("div", class_="mb-ldp__more-dtl__list--label")
                            value = li.find("div", class_="mb-ldp__more-dtl__list--value")
                            if label and value:
                                if "Age of Construction" in label.text:
                                    ageOfConstruction = value.text.strip()

                    # Scraping summary details from the property card
                    summary_items = property_card.find_all('div', class_='mb-srp__card__summary__list--item')
                    for item in summary_items:
                        label = item.find('div', class_='mb-srp__card__summary--label')
                        value = item.find('div', class_='mb-srp__card__summary--value')
                        if label and value:
                            label_text = label.text.strip()
                            value_text = value.text.strip()
                            if "Carpet Area" in label_text:
                                carpet_area = value_text
                            elif "Status" in label_text:
                                status = value_text
                            elif "Floor" in label_text:
                                floor = value_text
                            elif "Transaction" in label_text:
                                transaction = value_text
                            elif "Furnishing" in label_text:
                                furnishing = value_text
                            elif "facing" in label_text:
                                facing = value_text
                            elif "overlooking" in label_text:
                                overlooking = value_text
                            elif "Society" in label_text:
                                society = value_text
                            elif "Bathroom" in label_text:
                                bathroom = value_text
                            elif "Balcony" in label_text:
                                balcony = value_text
                            elif "Car Parking" in label_text:
                                car_parking = value_text
                            elif "Ownership" in label_text:
                                ownership = value_text
                            elif "Super Area" in label_text:
                                super_area = value_text
                            elif "Dimensions" in label_text:
                                dimensions = value_text
                            elif "Plot Area" in label_text:
                                plot_area = value_text

                    print(f"City: {city_slug.replace('-', ' ').title()}, Title: {title}, Description: {description}, Amount: {amount} , price/sqft: {price_per_sqft}")
                    print(f"Posted Date: {posted_date}, Age of Construction: {ageOfConstruction}, Carpet Area: {carpet_area}")
                    print(f"Status: {status}, Floor: {floor}, Transaction: {transaction}, Furnishing: {furnishing}")
                    print(f"Facing: {facing}, Overlooking: {overlooking}, Society: {society}, Bathroom: {bathroom}")
                    print(f"Balcony: {balcony}, Car Parking: {car_parking}, Ownership: {ownership}, Super Area: {super_area}")
                    print(f"Dimensions: {dimensions}, Plot Area: {plot_area}")
                    properties_data.append({
                        'City': city_slug.replace("-", " ").title(),
                        'Title': title,
                        'Description': description,
                        'Amount': amount,
                        'Price/sqft': price_per_sqft,
                        'Posted Date': posted_date,
                        'Age of Construction': ageOfConstruction,
                        'Carpet Area': carpet_area,
                        'Status': status,
                        'Floor': floor,
                        'Transaction': transaction,
                        'Furnishing': furnishing,
                        'Facing': facing,
                        'Overlooking': overlooking,
                        'Society': society,
                        'Bathroom': bathroom,
                        'Balcony': balcony,
                        'Car Parking': car_parking,
                        'Ownership': ownership,
                        'Super Area': super_area,
                        'Dimensions': dimensions,
                        'Plot Area': plot_area
                    })
                except Exception as e:
                    print(f"Error fetching details page for {details_url}: {e}")

        return properties_data
    except Exception as e:
        print(f"Error fetching page {page_num} of city {city_slug}: {e}")
        return []

# Main script execution
with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_city_page = {}
    for city in cities:
        city_slug = city.replace(" ", "-").lower()
        formatted_city_name = city_slug.replace("-", " ").title()

        if formatted_city_name.lower() in map(str.lower, processed_cities):
            print(f"Skipping already processed city: {formatted_city_name}")
            continue

        # Fetch number of pages
        html_text = requests.get(f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs", headers=headers).text
        soup = BeautifulSoup(html_text, 'lxml')
        container = soup.find('div', class_='mb-srp__left')
        
        num_of_pages = 0

        if container:
            total_results_text = container.find("li", class_='mb-srp__tabs__list--item').text
            total_results = int(re.findall(r"\d{1,3}(?:,\d{3})*", total_results_text)[0].replace(",", ""))
            num_of_pages = (total_results // 30) + 1

        if num_of_pages > 0:
        # Schedule tasks
            for page_num in range(1, num_of_pages + 1):
                future = executor.submit(fetch_page_data, city_slug, page_num)
                future_to_city_page[future] = (city, page_num)

        # Collect and save data
                # Collect data for the entire city
        city_data = []
        for future in as_completed(future_to_city_page):
            city, page_num = future_to_city_page[future]
            try:
                properties_data = future.result()
                city_data.extend(properties_data)
            except Exception as e:
                print(f"Error processing page {page_num} of {city}: {e}")

        # Save data for the city after all pages are processed
        if city_data:
            pd.DataFrame(city_data).to_csv(city_csv_path, mode="a", header=not os.path.isfile(city_csv_path), index=False)
            print(f"Data for {city} saved successfully.")
        
        # Clear the future_to_city_page dictionary for the next city
        future_to_city_page.clear()


print("Scraping completed.")


# from concurrent.futures import ThreadPoolExecutor, as_completed
# from bs4 import BeautifulSoup
# import requests
# import json
# import pandas as pd
# import re
# import time
# import random
# import os

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win 64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36"
# }

# cities = []
# base_url = "https://www.magicbricks.com/property-for-sale-rent-in-Pune/residential-real-estate-Pune"
# html_text = requests.get(base_url, headers=headers).text
# soup = BeautifulSoup(html_text, "lxml")

# # Extract city names only once
# container = soup.find("div", class_='city-drop-lt')
# if container:
#     cities = [item.text.strip() for item in container.find_all('li')]

# # Check for existing data to avoid duplicate cities
# existing_data = pd.DataFrame()
# city_csv_path = "additional_data3.csv"

# # Load existing data if CSV already exists
# if os.path.isfile(city_csv_path):
#     existing_data = pd.read_csv(city_csv_path)
#     processed_cities = existing_data['City'].unique().tolist()
# else:
#     processed_cities = []

# print(f"Already processed cities: {processed_cities}")

# def fetch_page_data(city_slug, page_num):
#     url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs/page-{page_num}"
#     page_html_text = requests.get(url, headers=headers).text
#     page_soup = BeautifulSoup(page_html_text, "lxml")
#     properties_container = page_soup.find_all('div', class_='mb-srp__card')
#     properties_data = []

#     for property_card in properties_container:
#         title_tag = property_card.find('h2', class_='mb-srp__card--title')
#         title = title_tag.text.strip() if title_tag else "No Title"

#         amount = property_card.find("div", class_ = 'mb-srp__card__price--amount')
#         amount = amount.text if amount else None

#         price_per_sqft = property_card.find("div", class_ = 'mb-srp__card__price--size')
#         price_per_sqft = price_per_sqft.text if price_per_sqft else None


#         description = property_card.find("div", class_='mb-srp__card--desc--text')
#         description = description.text if description else None
#         script_tag = property_card.find('script', type="application/ld+json")
#         details_url = None
#         ageOfConstruction = None
#         carpet_area = None
#         status = None
#         floor = None
#         transaction = None
#         furnishing = None
#         facing = None
#         overlooking = None
#         society = None
#         bathroom = None
#         balcony = None
#         car_parking = None
#         ownership = None
#         super_area = None
#         dimensions = None
#         plot_area = None

#         json_data = json.loads(script_tag.string) if script_tag else None
#         details_url = json_data.get("url") if json_data else None

#         if details_url:
#             # Request the details page
#             detail_html_text = requests.get(details_url, headers=headers).text
#             detail_soup = BeautifulSoup(detail_html_text, 'lxml')
#             posted_date = detail_soup.find('span', class_="mb-ldp__posted--date")
#             posted_date = posted_date.text.strip() if posted_date else "No Posted Date"

#             details_container = detail_soup.find('section', id="more-details")
#             if details_container:
#                 li_tags = detail_soup.find_all('li', class_="mb-ldp__more-dtl__list--item")
#                 for li in li_tags:
#                     label_div = li.find("div", class_="mb-ldp__more-dtl__list--label")
#                     if label_div and "Age of Construction" in label_div.text:
#                         value_div = li.find("div", class_='mb-ldp__more-dtl__list--value')
#                         ageOfConstruction = value_div.text.strip() if value_div else None

#             # Scraping additional details like Carpet Area, Status, etc.
#             summary_items = property_card.find_all('div', class_='mb-srp__card__summary__list--item')
#             for item in summary_items:
#                 label = item.find('div', class_='mb-srp__card__summary--label')
#                 value = item.find('div', class_='mb-srp__card__summary--value')
#                 if label and value:
#                     label_text = label.text.strip()
#                     value_text = value.text.strip()
#                     if "Carpet Area" in label_text:
#                         carpet_area = value_text
#                     elif "Status" in label_text:
#                         status = value_text
#                     elif "Floor" in label_text:
#                         floor = value_text
#                     elif "Transaction" in label_text:
#                         transaction = value_text
#                     elif "Furnishing" in label_text:
#                         furnishing = value_text
#                     elif "facing" in label_text:
#                         facing = value_text
#                     elif "overlooking" in label_text:
#                         overlooking = value_text
#                     elif "Society" in label_text:
#                         society = value_text
#                     elif "Bathroom" in label_text:
#                         bathroom = value_text
#                     elif "Balcony" in label_text:
#                         balcony = value_text
#                     elif "Car Parking" in label_text:
#                         car_parking = value_text
#                     elif "Ownership" in label_text:
#                         ownership = value_text
#                     elif "Super Area" in label_text:
#                         super_area = value_text
#                     elif "Dimensions" in label_text:
#                         dimensions = value_text
#                     elif "Plot Area" in label_text:
#                         plot_area = value_text

#             # Print the data (for testing purposes)
#             print(f"City: {city_slug.replace('-', ' ').title()}, Title: {title}, Description: {description}, Amount: {amount} , price/sqft: {price_per_sqft}")
#             print(f"Posted Date: {posted_date}, Age of Construction: {ageOfConstruction}, Carpet Area: {carpet_area}")
#             print(f"Status: {status}, Floor: {floor}, Transaction: {transaction}, Furnishing: {furnishing}")
#             print(f"Facing: {facing}, Overlooking: {overlooking}, Society: {society}, Bathroom: {bathroom}")
#             print(f"Balcony: {balcony}, Car Parking: {car_parking}, Ownership: {ownership}, Super Area: {super_area}")
#             print(f"Dimensions: {dimensions}, Plot Area: {plot_area}")

#             properties_data.append({
#                 'City': city_slug.replace("-", " ").title(),
#                 'Title': title,
#                 'Description': description,
#                 'Amount': amount,
#                 'Price/sqft': price_per_sqft,
#                 'Posted Date': posted_date,
#                 'Age of Construction': ageOfConstruction,
#                 'Carpet Area': carpet_area,
#                 'Status': status,
#                 'Floor': floor,
#                 'Transaction': transaction,
#                 'Furnishing': furnishing,
#                 'Facing': facing,
#                 'Overlooking': overlooking,
#                 'Society': society,
#                 'Bathroom': bathroom,
#                 'Balcony': balcony,
#                 'Car Parking': car_parking,
#                 'Ownership': ownership,
#                 'Super Area': super_area,
#                 'Dimensions': dimensions,
#                 'Plot Area': plot_area,
#             })
#     return properties_data

# # Main script execution
# with ThreadPoolExecutor(max_workers=10) as executor:
#     future_to_city_page = {}
    
#     for city in cities:
#         # Check if the city has already been processed
#         city_slug = city.replace(" ", "-").lower()
#         formatted_city_name = city_slug.replace("-", " ").title()
#         if formatted_city_name in processed_cities:
#             print(f"Skipping already processed city: {formatted_city_name}")
#             continue

#         city_data = []
#         html_text = requests.get(f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs", headers=headers).text
#         soup = BeautifulSoup(html_text, 'lxml')
#         container = soup.find('div', class_='mb-srp__left')

#         if container:
#             total_results_text = container.find("li", class_='mb-srp__tabs__list--item').text
#             total_results = int(re.findall(r"\d{1,3}(?:,\d{3})*", total_results_text)[0].replace(",", ""))
#             num_of_pages = (total_results // 30) + 1
 

#             # Limit the scraping to only the first two pages for testing
#         for page_num in range(1,  num_of_pages + 1):  # min(3, ...) ensures we only take 1 and 2
#             future = executor.submit(fetch_page_data, city_slug, page_num)
#             future_to_city_page[future] = (city, page_num)

#         # Collect and save data for the current city
#         for future in as_completed(future_to_city_page):
#             city, page_num = future_to_city_page[future]
#             try:
#                 properties_data = future.result()
#                 city_data.extend(properties_data)
#                 print(f"Scraped page {page_num} of {city} successfully.")
#             except Exception as e:
#                 print(f"Error scraping page {page_num} of {city}: {e}")

#         # Save current city's data to CSV
#         if city_data:
#             city_df = pd.DataFrame(city_data)

#             if not os.path.isfile(city_csv_path):
#                 city_df.to_csv(city_csv_path, mode="w", index=False)
#             else:
#                 city_df.to_csv(city_csv_path, mode="a", header=False, index=False)
#             print(f"Saved data for {city} to CSV.")
        
#         future_to_city_page.clear()

# print("Scrapping completed")





# from concurrent.futures import ThreadPoolExecutor, as_completed
# from bs4 import BeautifulSoup
# import requests
# import json
# import pandas as pd
# import re
# import time
# import random
# import os

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win 64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36"
# }


# cities = []
# base_url = "https://www.magicbricks.com/property-for-sale-rent-in-Pune/residential-real-estate-Pune"
# html_text = requests.get(base_url, headers=headers).text
# soup = BeautifulSoup(html_text, "lxml")

# # Extract city names only once
# container = soup.find("div", class_='city-drop-lt')
# if container:
#     cities = [item.text.strip() for item in container.find_all('li')]

# # Check for existing data to avoid duplicate cities
# existing_data = pd.DataFrame()
# city_csv_path = "additional_data3.csv"

# # Load existing data if CSV already exists
# if os.path.isfile(city_csv_path):
#     existing_data = pd.read_csv(city_csv_path)
#     # Extract the list of already processed cities
#     processed_cities = existing_data['City'].unique().tolist()
# else:
#     processed_cities = []

# print(f"Already processed cities: {processed_cities}")

# def fetch_page_data(city_slug, page_num):
#     url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs/page-{page_num}"
#     page_html_text = requests.get(url, headers=headers).text
#     page_soup = BeautifulSoup(page_html_text, "lxml")
#     properties_container = page_soup.find_all('div', class_='mb-srp__card')
#     properties_data = []

#     for property_card in properties_container:
#         title_tag = property_card.find('h2', class_='mb-srp__card--title')
#         title = title_tag.text.strip() if title_tag else "No Title"

#         description = property_card.find("div", class_='mb-srp__card--desc--text')
#         description = description.text if description else None
#         script_tag = property_card.find('script', type="application/ld+json")
#         details_url = None
#         ageOfConstruction = None

#         json_data = json.loads(script_tag.string) if script_tag else None
#         details_url = json_data.get("url") if json_data else None

#         if details_url:
#             # Request the details page
#             detail_html_text = requests.get(details_url, headers=headers).text
#             detail_soup = BeautifulSoup(detail_html_text, 'lxml')
#             posted_date = detail_soup.find('span', class_="mb-ldp__posted--date")
#             posted_date = posted_date.text.strip() if posted_date else "No Posted Date"

#             details_container = detail_soup.find('section', id="more-details")
#             if details_container:
#                 li_tags = detail_soup.find_all('li', class_="mb-ldp__more-dtl__list--item")
#                 for li in li_tags:
#                     label_div = li.find("div", class_="mb-ldp__more-dtl__list--label")
#                     if label_div and "Age of Construction" in label_div.text:
#                         value_div = li.find("div", class_='mb-ldp__more-dtl__list--value')
#                         ageOfConstruction = value_div.text.strip() if value_div else None

#             # Print Age of Construction and Posted Date
#             print(f"City: {city_slug.replace('-', ' ').title()}, Title: {title}, Description: {description}")
#             print(f"Posted Date: {posted_date}, Age of Construction: {ageOfConstruction}")

#             properties_data.append({
#                 'City': city_slug.replace("-", " ").title(),
#                 'Title': title,
#                 'Description': description,
#                 'Posted Date': posted_date,
#                 'Age of Construction': ageOfConstruction,
#             })
#     return properties_data

# # Main script execution
# with ThreadPoolExecutor(max_workers=10) as executor:
#     future_to_city_page = {}
#     city_index_to_start = 3  # Start from the fourth city (0-based index, so index 3 is the fourth)

#     for i, city in enumerate(cities):
#         if i < city_index_to_start:
#             continue  # Skip the first three cities

#         # Check if the city has already been processed
#         city_slug = city.replace(" ", "-").lower()
#         formatted_city_name = city_slug.replace("-", " ").title()
#         if formatted_city_name in processed_cities:
#             print(f"Skipping already processed city: {formatted_city_name}")
#             continue

#         city_data = []
#         html_text = requests.get(f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs", headers=headers).text
#         soup = BeautifulSoup(html_text, 'lxml')
#         container = soup.find('div', class_='mb-srp__left')

#         if container:
#             total_results_text = container.find("li", class_='mb-srp__tabs__list--item').text
#             total_results = int(re.findall(r"\d{1,3}(?:,\d{3})*", total_results_text)[0].replace(",", ""))
#             num_of_pages = (total_results // 30) + 1

#             for page_num in range(1, num_of_pages + 1):
#                 future = executor.submit(fetch_page_data, city_slug, page_num)
#                 future_to_city_page[future] = (city, page_num)

#         # Collect and save data for the current city
#         for future in as_completed(future_to_city_page):
#             city, page_num = future_to_city_page[future]
#             try:
#                 properties_data = future.result()
#                 city_data.extend(properties_data)
#                 print(f"Scraped page {page_num} of {city} successfully.")
#             except Exception as e:
#                 print(f"Error scraping page {page_num} of {city}: {e}")

#         # Save current city's data to CSV
#         if city_data:
#             city_df = pd.DataFrame(city_data)

#             if not os.path.isfile(city_csv_path):
#                 city_df.to_csv(city_csv_path, index=False)
#             else:
#                 city_df.to_csv(city_csv_path, mode='a', header=False, index=False)

#             print(f"Data for {city} saved to {city_csv_path}.")

#         # Clear the future dictionary for the next city
#         future_to_city_page.clear()

# print("All data scraping completed.")

# from concurrent.futures import ThreadPoolExecutor, as_completed
# from bs4 import BeautifulSoup
# import requests
# import json
# import pandas as pd
# import re
# import time
# import random
# import os

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win 64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36"
# }

# scraped_data = []
# cities = []
# base_url = "https://www.magicbricks.com/property-for-sale-rent-in-Pune/residential-real-estate-Pune"
# html_text = requests.get(base_url, headers=headers).text
# soup = BeautifulSoup(html_text, "lxml")

# # Extract city names only once
# container = soup.find("div", class_='city-drop-lt')
# if container:
#     cities = [item.text.strip() for item in container.find_all('li')]

# def fetch_page_data(city_slug, page_num):
#     url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs/page-{page_num}"
#     page_html_text = requests.get(url, headers=headers).text
#     page_soup = BeautifulSoup(page_html_text, "lxml")
#     properties_container = page_soup.find_all('div', class_='mb-srp__card')
#     properties_data = []

#     for property_card in properties_container:
#         title_tag = property_card.find('h2', class_='mb-srp__card--title')
#         title = title_tag.text.strip() if title_tag else "No Title"

#         description = property_card.find("div", class_ = 'mb-srp__card--desc--text')
#         description = description.text if description else None
#         script_tag = property_card.find('script', type="application/ld+json")
#         details_url = None
#         ageOfConstruction = None

#         json_data = json.loads(script_tag.string) if script_tag else None
#         details_url = json_data.get("url") if json_data else None

#         if details_url:
#             # Request the details page
#             detail_html_text = requests.get(details_url, headers=headers).text
#             detail_soup = BeautifulSoup(detail_html_text, 'lxml')
#             posted_date = detail_soup.find('span', class_="mb-ldp__posted--date")
#             posted_date = posted_date.text.strip() if posted_date else "No Posted Date"

#             details_container = detail_soup.find('section', id="more-details")
#             if details_container:
#                 li_tags = detail_soup.find_all('li', class_="mb-ldp__more-dtl__list--item")
#                 for li in li_tags:
#                     label_div = li.find("div", class_="mb-ldp__more-dtl__list--label")
#                     if label_div and "Age of Construction" in label_div.text:
#                         value_div = li.find("div", class_='mb-ldp__more-dtl__list--value')
#                         ageOfConstruction = value_div.text.strip() if value_div else None

#             # Print Age of Construction and Posted Date
#             print(f"City: {city_slug.replace('-', ' ').title()}, Title: {title}, Description:{description}")
#             print(f"Posted Date: {posted_date}, Age of Construction: {ageOfConstruction}")

#             properties_data.append({
#                 'City': city_slug.replace("-", " ").title(),
#                 'Title': title,
#                 'Description': description,
#                 'Posted Date': posted_date,
#                 'Age of Construction': ageOfConstruction,
#             })
#     return properties_data

# # Main script execution
# with ThreadPoolExecutor(max_workers=10) as executor:
#     future_to_city_page = {}
#     city_index_to_start = 3  # Start from the fourth city (0-based index, so index 3 is the fourth)

#     for i, city in enumerate(cities):
#         if i < city_index_to_start:
#             continue  # Skip the first three cities

#         city_slug = city.replace(" ", "-").lower()
#         city_data = []
#         html_text = requests.get(f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs", headers=headers).text
#         soup = BeautifulSoup(html_text, 'lxml')
#         container = soup.find('div', class_='mb-srp__left')

#         if container:
#             total_results_text = container.find("li", class_='mb-srp__tabs__list--item').text
#             total_results = int(re.findall(r"\d{1,3}(?:,\d{3})*", total_results_text)[0].replace(",", ""))
#             num_of_pages = (total_results // 30) + 1

#             for page_num in range(1, num_of_pages + 1):
#                 future = executor.submit(fetch_page_data, city_slug, page_num)
#                 future_to_city_page[future] = (city, page_num)

#         # Collect and save data for the current city
#         for future in as_completed(future_to_city_page):
#             city, page_num = future_to_city_page[future]
#             try:
#                 properties_data = future.result()
#                 city_data.extend(properties_data)
#                 print(f"Scraped page {page_num} of {city} successfully.")
#             except Exception as e:
#                 print(f"Error scraping page {page_num} of {city}: {e}")

#         # Save current city's data to CSV
#         if city_data:
#             city_csv_path = "additional_data2.csv"
#             city_df = pd.DataFrame(city_data)

#             if not os.path.isfile(city_csv_path):
#                 city_df.to_csv(city_csv_path, index=False)
#             else:
#                 city_df.to_csv(city_csv_path, mode='a', header=False, index=False)

#             print(f"Data for {city} saved to {city_csv_path}.")

#         # Clear the future dictionary for the next city
#         future_to_city_page.clear()

# print("All data scraping completed.")


# from concurrent.futures import ThreadPoolExecutor, as_completed
# from bs4 import BeautifulSoup
# import requests
# import json
# import pandas as pd
# import re
# import os
# import time
# import random

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win 64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36"
# }

# # Initialize variables
# scraped_data = []
# cities = []
# base_url = "https://www.magicbricks.com/property-for-sale-rent-in-Pune/residential-real-estate-Pune"
# html_text = requests.get(base_url, headers=headers).text
# soup = BeautifulSoup(html_text, "lxml")

# # Extract city names only once
# container = soup.find("div", class_='city-drop-lt')
# if container:
#     cities = [item.text.strip() for item in container.find_all('li')]

# def fetch_page_data(city_slug, page_num):
#     url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs/page-{page_num}"
#     page_html_text = requests.get(url, headers=headers).text
#     page_soup = BeautifulSoup(page_html_text, "lxml")
#     properties_container = page_soup.find_all('div', class_='mb-srp__card')
#     properties_data = []

#     for property_card in properties_container:
#         title_tag = property_card.find('h2', class_='mb-srp__card--title')
#         title = title_tag.text.strip() if title_tag else "No Title"
#         script_tag = property_card.find('script', type="application/ld+json")
#         details_url = None
#         ageOfConstruction = None

#         json_data = json.loads(script_tag.string) if script_tag else None
#         details_url = json_data.get("url") if json_data else None

#         if details_url:
#             # Request the details page
#             detail_html_text = requests.get(details_url, headers=headers).text
#             detail_soup = BeautifulSoup(detail_html_text, 'lxml')
#             posted_date = detail_soup.find('span', class_="mb-ldp__posted--date")
#             posted_date = posted_date.text.strip() if posted_date else "No Posted Date"

#             # Extract Age of Construction
#             details_container = detail_soup.find('section', id="more-details")
#             if details_container:
#                 li_tags = detail_soup.find_all('li', class_="mb-ldp__more-dtl__list--item")
#                 for li in li_tags:
#                     label_div = li.find("div", class_="mb-ldp__more-dtl__list--label")
#                     if label_div and "Age of Construction" in label_div.text:
#                         value_div = li.find("div", class_='mb-ldp__more-dtl__list--value')
#                         ageOfConstruction = value_div.text.strip() if value_div else None

#             # Append property data
#             properties_data.append({
#                 'City': city_slug.replace("-", " ").title(),
#                 'Title': title,
#                 'Posted Date': posted_date,
#                 'Age of Construction': ageOfConstruction,
#             })

#             # Print to monitor progress
#             print(f"City: {city_slug.replace('-', ' ').title()}, Title: {title}")
#             print(f"Posted Date: {posted_date}, Age of Construction: {ageOfConstruction}")

#     return properties_data

# # Main script execution
# with ThreadPoolExecutor(max_workers=10) as executor:
#     future_to_city_page = {}
#     for city in cities:
#         city_slug = city.replace(" ", "-").lower()
#         html_text = requests.get(f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs", headers=headers).text
#         soup = BeautifulSoup(html_text, 'lxml')
#         container = soup.find('div', class_='mb-srp__left')

#         if container:
#             total_results_text = container.find("li", class_='mb-srp__tabs__list--item').text
#             total_results = int(re.findall(r"\d{1,3}(?:,\d{3})*", total_results_text)[0].replace(",", ""))
#             num_of_pages = (total_results // 30) + 1

#             # Scrape each page for the current city
#             for page_num in range(1, num_of_pages + 1):
#                 future = executor.submit(fetch_page_data, city_slug, page_num)
#                 future_to_city_page[future] = (city, page_num)

#         # Save data incrementally after scraping each city's pages
#         for future in as_completed(future_to_city_page):
#             city, page_num = future_to_city_page[future]
#             try:
#                 properties_data = future.result()

#                 # Check if properties_data is not empty
#                 if properties_data:
#                     # Convert the scraped data to a DataFrame
#                     df = pd.DataFrame(properties_data)

#                     # Check if CSV file already exists
#                     file_exists = os.path.isfile("additional_data2.csv")

#                     # Save to CSV: if file doesn't exist, write with headers; if it does, append without headers
#                     df.to_csv("additional_data2.csv", mode='a', index=False, header=not file_exists)

#                     print(f"Data for page {page_num} of {city_slug.title()} saved to 'additional_data2.csv'.")
#                 else:
#                     print(f"No data found on page {page_num} of {city_slug.title()}.")

#             except Exception as e:
#                 print(f"Error scraping page {page_num} of {city}: {e}")

#         # Clear the future_to_city_page dictionary to free memory
#         future_to_city_page.clear()

# print("All data has been saved to 'additional_data2.csv'.")

# from concurrent.futures import ThreadPoolExecutor, as_completed
# from bs4 import BeautifulSoup
# import requests
# import json
# import pandas as pd
# import re
# import time
# import random
# import os

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win 64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36"
# }

# # scraped_data = []
# scraped_data = []
# cities = []
# base_url = "https://www.magicbricks.com/property-for-sale-rent-in-Pune/residential-real-estate-Pune"
# html_text = requests.get(base_url, headers=headers).text
# soup = BeautifulSoup(html_text, "lxml")

# # Extract city names only once
# container = soup.find("div", class_='city-drop-lt')
# if container:
#     cities = [item.text.strip() for item in container.find_all('li')]

# def fetch_page_data(city_slug, page_num):
#     url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs/page-{page_num}"
#     page_html_text = requests.get(url, headers=headers).text
#     page_soup = BeautifulSoup(page_html_text, "lxml")
#     properties_container = page_soup.find_all('div', class_='mb-srp__card')
#     properties_data = []

#     for property_card in properties_container:
#         title_tag = property_card.find('h2', class_='mb-srp__card--title')
#         title = title_tag.text.strip() if title_tag else "No Title"
#         script_tag = property_card.find('script', type="application/ld+json")
#         details_url = None
#         ageOfConstruction = None

#         json_data = json.loads(script_tag.string) if script_tag else None
#         details_url = json_data.get("url") if json_data else None

#         if details_url:
#             # Request the details page
#             detail_html_text = requests.get(details_url, headers=headers).text
#             detail_soup = BeautifulSoup(detail_html_text, 'lxml')
#             posted_date = detail_soup.find('span', class_="mb-ldp__posted--date")
#             posted_date = posted_date.text.strip() if posted_date else "No Posted Date"

#             details_container = detail_soup.find('section', id="more-details")
#             if details_container:
#                 li_tags = detail_soup.find_all('li', class_="mb-ldp__more-dtl__list--item")
#                 for li in li_tags:
#                     label_div = li.find("div", class_="mb-ldp__more-dtl__list--label")
#                     if label_div and "Age of Construction" in label_div.text:
#                         value_div = li.find("div", class_='mb-ldp__more-dtl__list--value')
#                         ageOfConstruction = value_div.text.strip() if value_div else None

#             # Print Age of Construction and Posted Date
#             print(f"City: {city_slug.replace('-', ' ').title()}, Title: {title}")
#             print(f"Posted Date: {posted_date}, Age of Construction: {ageOfConstruction}")

#             properties_data.append({
#                 'City': city_slug.replace("-", " ").title(),
#                 'Title': title,
#                 'Posted Date': posted_date,
#                 'Age of Construction': ageOfConstruction,
#             })
#     return properties_data

# # Main script execution
# with ThreadPoolExecutor(max_workers=10) as executor:
#     future_to_city_page = {}
#     for city in cities:
#         city_slug = city.replace(" ", "-").lower()
#         html_text = requests.get(f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs", headers=headers).text
#         soup = BeautifulSoup(html_text, 'lxml')
#         container = soup.find('div', class_='mb-srp__left')

#         if container:
#             total_results_text = container.find("li", class_='mb-srp__tabs__list--item').text
#             total_results = int(re.findall(r"\d{1,3}(?:,\d{3})*", total_results_text)[0].replace(",", ""))
#             num_of_pages = (total_results // 30) + 1

#             for page_num in range(1, num_of_pages + 1):
                
#                 if page_num == 1:
#                     url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs"
#                 else:
#                     url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs/page-{page_num}"
                    

#                 future = executor.submit(fetch_page_data, city_slug, page_num)
#                 future_to_city_page[future] = (city, page_num)

#     for future in as_completed(future_to_city_page):
#         city, page_num = future_to_city_page[future]
#         try:
#             properties_data = future.result()
#             scraped_data.extend(properties_data)
#             print(f"Scraped page {page_num} of {city} successfully.")
#         except Exception as e:
#             print(f"Error scraping page {page_num} of {city}: {e}")

#     if scraped_data:
#         city_csv_path = "additional_data2.csv"
#         city_df = pd.DataFrame(scraped_data)

#         if not os.path.isfile(city_csv_path):
#             city_df.to_csv(city_csv_path, index=False)
#         else:
#             city_df.to_csv(city_csv_path,mode='a',header=False, index=False)
        
#         print(f"data for {city}, saved to {city_csv_path}")

#     future_to_city_page.clear()
# # Convert to DataFrame
# # df = pd.DataFrame(scraped_data)
# # df.to_csv("additional_data2.csv", index=False)
# print("Scrapping completed")

# # from bs4 import BeautifulSoup
# # import requests
# # import re
# # import json
# # import time
# # import pandas as pd

# # headers = {
# #     "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win 64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36"
# # }

# # scraped_data = []
# # cities = []

# # # Base URL for Pune to scrap all cities name
# # base_url = "https://www.magicbricks.com/property-for-sale-rent-in-Pune/residential-real-estate-Pune"
# # html_text = requests.get(base_url, headers=headers).text
# # soup = BeautifulSoup(html_text, "lxml")

# # # Extract city names only once
# # container = soup.find("div", class_='city-drop-lt')
# # if container:
# #     cities = [item.text.strip() for item in container.find_all('li')]

# # # Loop through each city to fetch property URLs and details
# # for city in cities:
# #     city_slug = city.replace(" ", "-").lower()
# #     url = f"https://www.magicbricks.com/ready-to-move-flats-in-{city_slug}-pppfs"
    
    
  
# #     html_text = requests.get(url, headers=headers).text
# #     soup = BeautifulSoup(html_text, 'lxml')
# #     container = soup.find('div', class_='mb-srp__left')
        
# #         # Only calculate total results once for each city
# #     if container:
# #         total_results_text = container.find("li", class_='mb-srp__tabs__list--item').text
# #         total_results = int(re.findall(r"\d{1,3}(?:,\d{3})*", total_results_text)[0].replace(",", ""))
# #         num_of_pages = (total_results // 30) + 1

# #         for i in range(1, num_of_pages + 1):
# #                 # Build the page URL
# #             page_url = f"{url}/page-{i}" if i > 1 else url
# #             page_html_text = requests.get(page_url, headers=headers).text
# #             page_soup = BeautifulSoup(page_html_text, "lxml")


# #             # Extract property cards from the current page
# #             properties_container = page_soup.find_all('div', class_='mb-srp__card')
# #             for property_card in properties_container:
# #                 title_tag = property_card.find('h2', class_='mb-srp__card--title')
# #                 title = title_tag.text.strip() if title_tag else "No Title" 
# #                 script_tag = property_card.find('script', type="application/ld+json")
# #                 details_url = None
# #                 ageOfConstruction = None
                

# #                 json_data = json.loads(script_tag.string) if script_tag else None
# #                 details_url = json_data.get("url")
                


# #                 if details_url:
# #         # Request the details page only if the URL is available
# #                     detail_html_text = requests.get(details_url, headers=headers).text
# #                     detail_soup = BeautifulSoup(detail_html_text, 'lxml')
        
# #         # Scrape Posted Date
# #                     posted_date = detail_soup.find('span', class_="mb-ldp__posted--date")  # Update with actual class
# #                     posted_date = posted_date.text.strip() if posted_date else "No Posted Date"

# #         # Scrape Age of Construction
# #                     ageOfConstruction = None
# #                     details_container = detail_soup.find('section' , id = "more-details")
# #                     if details_container:
# #                         li_tags = detail_soup.find_all('li', class_ = "mb-ldp__more-dtl__list--item")

# #                         for li in li_tags:
# #                             label_div = li.find("div", class_ = "mb-ldp__more-dtl__list--label")
# #                             if label_div and "Age of Construction" in label_div.text:
# #                                 value_div = li.find("div", class_ = 'mb-ldp__more-dtl__list--value')

# #                                 if value_div:
# #                                     ageOfConstruction = value_div.text.strip()
# #                                 else:
# #                                     None

# #         # Append data to the list
# #                     scraped_data.append({
# #                         'City': city,
# #                         'Title': title,
# #                         'Posted Date': posted_date,
# #                         'Age of Construction': ageOfConstruction,
# #                         'Details URL': details_url
# #                     })
        
# #         # Print progress if desired
# #                     print(f" Posted Date - {posted_date}, Age of Construction - {ageOfConstruction}")
        
# #     # Optional: Add a short delay to avoid overwhelming the server
# #                 time.sleep(1)
    
# # # Convert the scraped data to a DataFrame
# # df = pd.DataFrame(scraped_data)
# # print(df)

#                 # if script_tag:
#                 #   # Parse JSON data for URL field only once
#                 #     json_data = json.loads(script_tag.string)
#                 #     details_url = json_data.get("url")
#                 # posted_dates = []
#                 # ageOfConstruction = []
#                 # details_html_text = requests.get(details_url, headers= headers).text
#                 # details_soup = BeautifulSoup(html_text, "lxml")

#                 # posted_date_tag = details_soup.find_all('span', class_ = "mb-ldp__posted--date")
#                 # for property_date in posted_date_tag:
#                 #     posted_date = posted_date_tag.text.strip() if posted_date_tag else None
#                 #     posted_dates.append(posted_date)
                    






#                 # if details_url:  # Check if URL is valid
#                 #     details_html_text = requests.get(details_url, headers=headers).text
#                 #     details_soup = BeautifulSoup(details_html_text, 'lxml')

#                 #     posted_date_container = details_soup.find('div', class_="mb-ldp__posted")
#                 #     posted_date_tag = posted_date_container.find('span', class_='mb-ldp__posted--date') if posted_date_container else None
#                 #     posted_date = posted_date_tag.text.strip() if posted_date_tag else "Not Available"
#                 #     print(posted_date)

# #                         # Extract posted date


# #                         # Extract age of construction
# #                         details_container = details_soup.find('section', id="more-details")
# #                         if details_container:
# #                             li_tags = details_container.find_all('li', class_="mb-ldp__more-dtl__list--item")
# #                             for li in li_tags:
# #                                 label_div = li.find("div", class_="mb-ldp__more-dtl__list--label")
# #                                 if label_div and "Age of Construction" in label_div.text:
# #                                     value_div = li.find("div", class_='mb-ldp__more-dtl__list--value')
# #                                     ageOfConstruction = value_div.text.strip() if value_div else "Not Available"
# #                                     break

# #                     # Append data to scraped_data
# #                     scraped_data.append({
# #                         "City": city,
# #                         "Title": title,
# #                         "Posted date": posted_date,
# #                         "Age of Construction": ageOfConstruction
# #                     })

# #                     # Reduce the sleep time for smoother but faster data gathering
# #                     time.sleep(0.2)
                    


# # # Convert scraped data to a DataFrame if needed
# # df = pd.DataFrame(scraped_data)

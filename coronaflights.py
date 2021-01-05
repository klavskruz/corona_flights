import requests
import pandas as pandas
import json
from datetime import datetime
import datetime
from datetime import timedelta
import sqlite3
import sqlalchemy
from sqlalchemy.orm import sessionmaker

DATABASE_LOCATION = "sqlite:///corona_flight_data.sqlite"

class CovidDataFinder:
    headers = {
    'x-rapidapi-key': "bf7aa826admsh914e2032ba16661p12d507jsnd32e0f8f48a5",
    'x-rapidapi-host': "covid1910.p.rapidapi.com"
    }
    def get_confirmed_cases(self ,country: str, date: str):
        url = f"https://covid1910.p.rapidapi.com/data/confirmed/country/{country}/date/{date}"
        response = requests.get(url, headers= self.headers)
        covid_data = response.json()
        try:
            print(country + "-" + date)
            return(covid_data[0]["confirmed"])
        except (IndexError,KeyError) as e:
            return 0
    def get_deaths(self, country: str, date: str):
        url = f"https://covid1910.p.rapidapi.com/data/death/country/{country}/date/{date}"
        response = requests.get(url, headers= self.headers)
        covid_data = response.json()
        try:
            covid_data[0]["death"]
            return(covid_data[0]["death"])
        except (IndexError,KeyError) as e:
            return 0
    def get_recovered(self, country: str, date: str):
        url = f"https://covid1910.p.rapidapi.com/data/recovered/country/{country}/date/{date}"
        response = requests.get(url, headers= self.headers)
        covid_data = response.json()
        try:
            covid_data[0]["recovered"]
            return(covid_data[0]["recovered"])
        except (IndexError,KeyError) as e:
            return 0



class FlightPriceFinder:
    BEARER_TOKEN = ""

    def __init__(self):
        self.BEARER_TOKEN = self.authenticate_amadeus()
        

    def authenticate_amadeus(self):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'grant_type': 'client_credentials',
            'client_id': 'PswWPQCMH9MMDPnD8IhoqjvpCUKD1hSA',
            'client_secret': 'mlUJuUXZVSLz5ve9'
                }

        response = requests.post('https://test.api.amadeus.com/v1/security/oauth2/token', headers=headers, data=data)
        if response:
            return response.json()['access_token']
        else:
            return "The authentication was unsuccesful"



    def get_flight_price(self, from_airport: str , to_airport: str, departure_date: str):
   
        headers = {
        'Authorization': f'Bearer {self.BEARER_TOKEN}',
        }
        try:
            response = requests.get(f"https://test.api.amadeus.com/v1/analytics/itinerary-price-metrics?originIataCode={from_airport}&destinationIataCode={to_airport}&departureDate={departure_date}&currencyCode=EUR&oneWay=true", headers= headers, verify = False)
        except ConnectionError:
             print("Connection could not be established")
        flight_data =  response.json()
        prices_of_the_day = {}
        for price in flight_data["data"][0]["priceMetrics"]:
            prices_of_the_day[price["quartileRanking"]] = float(price["amount"])
        return prices_of_the_day

    
        
            


class AirportDirectory:
    
    top_travel_destinations = {
        "New York":{"airport":"JFK","country":"us"},
        "Bejing":{"airport":"PEK","country":"China"},
        "Dubai":{"airport":"DXB","country":"United Arab Emirates"},
        "London":{"airport":"LHR","country":"United Kingdom"},
        "Paris":{"airport":"CDG","country":"France"},
        "Amsterdam":{"airport":"AMS","country":"Netherlands"},
        "Delhi":{"airport":"DEL","country":"India"},
        "Singapore":{"airport":"SIN","country":"Singapore"},
        "Bangkok":{"airport":"BKK","country":"Thailand"},
        "Madrid":{"airport":"MAD","country":"Spain"},
        "Istambul":{"airport":"IST","country":"Turkey"},
        "Toronto":{"airport":"YYZ","country":"Canada"},
        "Sydney":{"airport":"SYD","country":"Australia"},
        "Tokyo":{"airport":"HND","country":"Japan"}
        }
    
    test_origin_cities = {
        "Frankfurt":"FRA",
        "New York":"JFK",
        "Bejing":"PEK",
        "Dubai":"DXB",
        "London":"LHR",
        "Paris":"CDG",
        "Amsterdam":"AMS",
        "Delhi":"DEL",
        "Singapore":"SIN",
        "Bangkok":"BKK",
        "Madrid":"MAD",
        "Istambul":"IST",
        "Toronto":"YYZ",
        "Sydney":"SYD",
        "Tokyo":"HND"}

        }


def date_range_generator(day_count:int):
    base = datetime.datetime.today() - timedelta(days=1)
    date_list = [base - datetime.timedelta(days=x) for x in range(day_count)]
    date_list_formatted = []
    for date in date_list:
        date_list_formatted.append({"flight_format":date.strftime('%Y-%m-%d'),"covid_format":date.strftime('%m-%d-%Y')})
    date_list_formatted.reverse()
    return date_list_formatted



def retrieve_and_load(days_to_load: int, origin_city: str):
    airports = AirportDirectory()
    price_finder = FlightPriceFinder()
    covid_data_finder = CovidDataFinder()
    date_range = date_range_generator(days_to_load)
    data = {}
    result_tables = {}
    for destination in airports.top_travel_destinations:
        data[f"{destination}"] = []
        for date in date_range:
            current_date = []
            try:
                prices = price_finder.get_flight_price(from_airport = airports.test_origin_cities[origin_city], to_airport = airports.top_travel_destinations[destination]["airport"], departure_date = date["flight_format"])
            except Exception as e:
                print("FLIGHT PRICE ERROR")
                prices =  {"MINIMUM":0.0, "FIRST":0.0, "MEDIUM" : 0.0, "THIRD": 0.0, "MAXIMUM": 0.0}
                pass
            price_min = prices["MINIMUM"]
            price_first = prices["FIRST"]
            price_mid = prices["MEDIUM"]
            price_third = prices["THIRD"]
            price_max = prices["MAXIMUM"]
            price_average = sum(prices.values()) / 5
            try:
                total = covid_data_finder.get_confirmed_cases(airports.top_travel_destinations[destination]["country"],date["covid_format"])
                deaths = covid_data_finder.get_deaths(airports.top_travel_destinations[destination]["country"],date["covid_format"])
                recovered = covid_data_finder.get_recovered(airports.top_travel_destinations[destination]["country"],date["covid_format"])
            except Exception as e:
                print("COVID DATA ERROR")
                pass
            active = total - recovered - deaths
            current_date.append(price_min)
            current_date.append(price_first)
            current_date.append(price_mid)
            current_date.append(price_third)
            current_date.append(price_max)
            current_date.append(price_average)
            current_date.append(total)
            current_date.append(deaths)
            current_date.append(recovered)
            current_date.append(active)
            current_date.append(date['flight_format'])
            data[destination].append(current_date)
    for city in data:
        result_tables[f"{city}"] = pandas.DataFrame(data[city],columns = ["Price-Min","Price-First","Price-Medium","Price-Third","Price-Maximum","Price-Average" , "Total","Deaths","Recovered","Active","Date"])
    for table in result_tables:
    load_table(table,result_tables[table])
    


#Load data in a database
def load_table(table_name_in: str, destination_df):
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("corona_flight_data.sqlite")
    cursor = conn.cursor()
    table_name = table_name_in

    try:
        destination_df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            dtype={
                "Price-Min": sqlalchemy.types.Float(),
                "Price-First":sqlalchemy.types.Float(),
                "Price-Medium":sqlalchemy.types.Float(),
                "Price-Third":sqlalchemy.types.Float(),
                "Price-Maximum":sqlalchemy.types.Float(),
                "Price-Average":sqlalchemy.types.Float(),
                "Total": sqlalchemy.types.INTEGER(),
                "Deaths": sqlalchemy.types.INTEGER(),
                "Recovered": sqlalchemy.types.INTEGER(),
                "Date": sqlalchemy.types.String(16),

            }
        )
    except:
        raise Exception("Something went wrong while loading data in the database")

    print("Data loaded successfuly")













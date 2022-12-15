import datetime
import time
from polygon import RESTClient
#from sqlalchemy import create_engine 
#from sqlalchemy import text
import pymongo
from dotenv import dotenv_values
import pandas as pd
import os
import pycaret.regression as pycr
import pycaret.utils as pycu
import numpy as np

# store key value pairs defined in .env file in config
config= dotenv_values(".env")
#pd.set_option('display.float_format', lambda x: '%.3f' % x)

class TrailingStopsTrainingData3:
    """
    Fetch Data from polygon API and store in sqllite database
    
    :param key: polygon.io key store in library credentials
    :type key: string

    :param currency_pairs: List contains list of trading currency pairs, upper and lower keltner bands, Total no of keltner bands crosses in six minutes window and initial investment amount in that six 
                            minutes window for that perticular currency pairs.
    :type currency_pair: List

    :param short : List of currency that we sell
    :type short: List

    :param long : List of currency that we bought
    :type long: List

    :param count : counter in seconds to check program hits 10 hours.
    :type count:  int

    :param agg_count: counter in seconds to check if 6 minutes has been reached or not 
    :type agg_count: int

    :param engine : Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
    :type engine: sqlalchemy.create_engine
    """

    # Init all the necessary variables when instantiating the class
    def __init__(self):
        self.currency_pairs = [ ["EUR","USD",{'upper': list(), 'lower': list()},0,100],
                                ["GBP","USD",{'upper': list(), 'lower': list()},0,100],
                                ["CHF","USD",{'upper': list(), 'lower': list()},0,100],
                                ["CAD","USD",{'upper': list(), 'lower': list()},0,100],
                                ["HKD","USD",{'upper': list(), 'lower': list()},0,100],
                                ["AUD","USD",{'upper': list(), 'lower': list()},0,100],
                                ["NZD","USD",{'upper': list(), 'lower': list()},0,100],
                                ["SGD","USD",{'upper': list(), 'lower': list()},0,100]
                             ]
        # API Key
        self.key= config['key']
        # create pymango client for mongodb 
        self.client = pymongo.MongoClient("127.0.0.1", 27017)
        # create or point to database in mongodb
        self.db= self.client['ForexData']

    # Function slightly modified from polygon sample code to format the date string
    """
    :param ts: timestamp that need to be formated
    :type ts: String 
    """
    def ts_to_datetime(self,ts) -> str:
        return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    # Function which clears the raw data tables once we have aggregated the data in a 6 minute interval
    def reset_raw_data_tables(self):
        for curr in self.currency_pairs:
            curr[3]=0 # it sets the item at index 3 in each currency pair list to zero after every six minutes
            collection= self.db[curr[0]+curr[1]+"_raw"]
            collection.delete_many({})

    # function will claculate 100 upper bands and 100 lower bands from avg_price for this six minute data and store in corresponding currency pair dictionary.
    """
    Parameters required for this function are-:
    curr-: list of perticular currency pair for which we are calculating keltner bands
    avg_price-: mean price for this six minute window
    volatility-: max_price- min_price for this six minute window
    """
    def calculate_keltner_bands(self, curr, avg_price, volatility):
        curr[2]['upper'].append(avg_price)
        curr[2]['lower'].append(avg_price)
        for i in range(100):
            upper_band= avg_price+ (i+1)*0.025*volatility
            lower_band= avg_price- (i+1)*0.025*volatility
            curr[2]['upper'].append(upper_band)
            curr[2]['lower'].append(lower_band)

    """
    This function calculate mean, max, min, volatility, keltner bands, fractal dimension and return for that six minute window.
    we store all these values in _agg tables for that currency pairs.
    
    """
    def aggregate_raw_data_tables(self):
        #with self.engine.begin() as conn:
        for curr in self.currency_pairs:
            collection= self.db[curr[0]+curr[1]+"_raw"]
            agg_collection= self.db[curr[0]+curr[1]+"_agg"] # create collection for _agg table data
            result= collection.aggregate([{"$group": {"_id": '', "avg_price": { "$avg": "$fxrate" }, "min_price": { "$min": "$fxrate" }, "max_price": { "$max": "$fxrate" }, "total_crosses": { "$sum": "$cross_number" },"last_date": { "$max": "$ticktime" }}}])
            for row in result:
                avg_price = row['avg_price'] # mean price for this six minutes data
                min_price = row['min_price'] # minimum price for this six minutes data
                max_price = row['max_price'] # maximum price for this six minutes data
                last_date = row['last_date']
                volatility = (max_price- min_price)/avg_price
                print(row['total_crosses'])
                if volatility!=0: # In case if their is no change in currency pair price in 6 minute window
                    fd= row['total_crosses']/ volatility # fractal dimension for this six minute data
                else:
                    fd=0
            curr[2]['upper'].clear() # clear the list to store recent bands value
            curr[2]['lower'].clear() # clear the list to store recent bands value
            self.calculate_keltner_bands(curr, avg_price, volatility) # function will claculate 100 upper bands and 100 lower bands from avg_price for this six minute data and store in corresponding currency pair dictionary. 
            prev_avg=0 # mean price for previous six minutes. For first six minutes of program execution, prev_agg will be zero.
            agg_data= agg_collection.find(sort=[( 'inserttime', pymongo.DESCENDING )] ).limit(1)
            for row in agg_data:
                if len(row) !=0: # For first six minutes of executing program, we will not have any entry in _agg tables. This will check if we have rows in_agg tables or not
                    prev_avg = row['avgfxrate']
            if prev_avg !=0: # for first six minutes of program execution, ret will be xero else it will be calculated by below formula.
                ret= (avg_price-prev_avg)/prev_avg
            else:
                ret=0
            print({"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd, "return": ret})
            # insert inside the corresponding currency pairs _agg tables
            agg_collection.insert_one({"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd, "return": ret})

    """
    Elements in upper band list are already sorted in ascending order while elements in lower band list are store in decending order.
    I am using binary search to serch index of larget element smaller than or equal to the current price in upper band and index of smallest element greater than or equal to current price in lower band.
    If current price is lower than avg price, we search in lower band list, other wise we search for index in upper band list.
    if index of upper band list is returned, we returned the index as positive number, otherwise we return it as negative.
    """
    def calculate_crosses(self, currency, avg_price):
        keltner_Ubands= currency[2]['upper']
        keltner_Lbands=  currency[2]['lower'] # list of 100 upper bands and 100 lower bands from previous six minutes data each of length 101
        length= len(keltner_Ubands)
        if avg_price < keltner_Ubands[0]:
            l=0 # first element index in the list
            h= length-1 # last element index in the list
            while l<h:
                mid= l+ int((h-l)/2) # average price of previous six minute data
                if avg_price > keltner_Lbands[mid]:
                    h= mid-1 # this is due elements are stored in decending order.
                else:
                    l=mid+1
            return -l # index of smallest band greater than or equal to the current price
        else:
            l=0 # first element index in the list
            h= length-1 # last element index in the list
            while l<h:
                mid= l+ int((h-l)/2) # average price of previous six minute data
                if avg_price > keltner_Ubands[mid]:
                    l= mid+1 # this is due to elements are stored in ascending order.
                else:
                    h= mid-1
            return l # index of largest band smaller or equal to the current price.

    # this function will fetch data from sqlite tables and create csv files for _agg, _sell and _bought tables
    def create_csv(self):
        basedir= os.path.abspath(os.path.dirname(__file__))
        for curr in self.currency_pairs:
            agg_collection= self.db[curr[0]+curr[1]+"_agg"]
            print(curr[0]+curr[1])
            print("------------------------------------------------")
            count_rows= agg_collection.aggregate([{"$group":{"_id":'',"total_rows": { "$sum": 1 }}}])
            for row in count_rows:
                print("total no of rows in table ",row['total_rows'])
                print("creating csv")
            results= pd.DataFrame(list(agg_collection.find()))
            results.to_csv(os.path.join(basedir, curr[0]+curr[1]+".csv"), index= False, sep=";")
                

    """
    This function fetch polygon data every second. We have two counters count and agg_count to check no of hours and no of minutes that our program has run.
    We reset agg_count every six minutes and call 

    """
    def trailing_forex_trainingdata3(self):
        # Number of list iterations - each one should last about 1 second
        count = 0 # counter to keep track of 10 hours execution
        agg_count = 0 # counter to keep track of six minute wndow
        
        # Open a RESTClient for making the api calls
        client= RESTClient(self.key) 
        
        # Loop that runs until the total duration of the program hits 10 hours. 
        while count < 36001: # 36000 seconds = 10 hours
            # check if program is executed for six minutes, if yes, call function aggregate_raw_data_tables() and reset_raw_data_tables() and reset agg_count to zero
            if agg_count == 360:
                # Aggregate the data and clear the raw data tables
                self.aggregate_raw_data_tables()
                self.reset_raw_data_tables()
                agg_count = 0
            time.sleep(0.10)
            # Increment the counters
            count += 1
            agg_count +=1
            # Loop through each currency pair
            for currency in self.currency_pairs:
                from_ = currency[0] 
                to = currency[1]
                # create collection to store _raw table data
                collection= self.db[from_+to+"_raw"]
                # Call the API with the required parameters
                try:
                    resp = client.get_real_time_currency_conversion(from_, to, amount=100, precision=2)
                except:
                    continue
                # This gets the Last Trade object defined in the API Resource
                last_trade = resp.last
                # Format the timestamp from the result
                dt = self.ts_to_datetime(last_trade.timestamp)
                # Get the current time and format it
                insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Calculate the price by taking the average of the bid and ask prices
                avg_price = (last_trade.bid + last_trade.ask)/2
                cross_number = 0 # no of keltner bands crossed by the current price fetched
                c_number=0 # relative no of keltner bands crossed with respect to previous price postion index.
                # after first six minutes we started claculating crosses for current fetch price 
                if count > 360: 
                    # return the index of element smaller than or equal to price from upper band list or greater than or equal to price from lower band list
                    cross_number =self.calculate_crosses(currency, avg_price)  
                    # subtract the previous index from current index to get the relative no of bands crossed.
                    # If previous index is in upper band list and current index is in lower band list, we get the addition of upper band index and lower band index as lower band index is retuned in negative and upper band index is returned as positive. 
                    #Simmilarly if both previous and current index in lies in the sams bands i.e upper band list or lower band list, current index will get subtracted from previous index.
                    c_number= cross_number- currency[3]
                    c_number= abs(c_number)
                    currency[3]= cross_number # store index of current price to calculate the relative position of next price
                # Write the data to raw data table collection
                collection.insert_one({"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time, "cross_number": c_number})


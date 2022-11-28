import datetime
import time
from polygon import RESTClient
from sqlalchemy import create_engine 
from sqlalchemy import text
from dotenv import dotenv_values
import pandas as pd
import os
# store key value pairs defined in .env file in config
config= dotenv_values(".env")

class KeltnerForexData:
    """
    Fetch Data from polygon API and store in sqllite database
    
    :param key: polygon.io key store in library credentials
    :type key: string

    :param currency_pairs: List contains list of trading currency pairs, upper and lower keltner bands, Total no of keltner bands crosses in six minutes window for that perticular currency pairs.
    :type currency_pairs: List

    :param count : counter in seconds to check program hits 10 hours.
    :type count:  int

    :param agg_count: counter in seconds to check if 6 minutes has been reached or not 
    :type agg_count: int

    :param engine : Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
    :type engine: sqlalchemy.create_engine
    """

    # Init all the necessary variables when instantiating the class
    def __init__(self):
        self.currency_pairs = [ ["AUD","USD",{'upper': list(), 'lower': list()},0],
                                ["EUR","USD",{'upper': list(), 'lower': list()},0],
                                ["CAD","USD",{'upper': list(), 'lower': list()},0],
                                ["JPY","USD",{'upper': list(), 'lower': list()},0],
                                ["CNY","USD",{'upper': list(), 'lower': list()},0],
                                ["GBP","USD",{'upper': list(), 'lower': list()},0],
                                ["MXN","USD",{'upper': list(), 'lower': list()},0],
                                ["CZK","USD",{'upper': list(), 'lower': list()},0],
                                ["PLN","USD",{'upper': list(), 'lower': list()},0],
                                ["INR","USD",{'upper': list(), 'lower': list()},0]
                             ]
        # API Key
        self.key= config['key']
        # create/ read SQL LITE DB file
        self.engine= create_engine("sqlite+pysqlite:///final_test.db", echo=False, future=False)

    # Function slightly modified from polygon sample code to format the date string
    """
    :param ts: timestamp that need to be formated
    :type ts: String
    """
    def ts_to_datetime(self,ts) -> str:
        return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    # Function which clears the raw data tables once we have aggregated the data in a 6 minute interval
    def reset_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                curr[3]=0 # it sets the item at index 3 in each currency pair list to zero after every six minutes
                conn.execute(text("DROP TABLE "+curr[0]+curr[1]+"_raw;"))
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_raw(ticktime text, fxrate  numeric, inserttime text, cross_number numeric );"))

    # This creates a table for storing the raw, unaggregated price data for each currency pair in the SQLite database
    def initialize_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_raw(ticktime text, fxrate  numeric, inserttime text, cross_number numeric);"))

    # This creates a table for storing the (6 min interval) aggregated price data for each currency pair in the SQLite database
    """
    This table contains following coloums
    inserttime-: Last tik time in raw table for that currency pairs
    avgfxrate-: mean price for that currency pair in that six minutes window
    min_price-: minimum price in which that currency pair is traded in that six minute window
    max_price-: maximum price in which that currency pair is traded in that six minute window
    volatility-: max_price minus min_price in that six minute window
    fractal_dimension -: Total number of keltner bands crosses in that six minute window divide by volatility
    """
    def initialize_aggregated_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_agg(inserttime text, avgfxrate  numeric, min_price numeric, max_price numeric, volatility numeric, fractal_dimension nuemric);"))

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
    his function calculate mean, max, min, volatility, keltner bands,and fractal dimension for that six minute window.
    we store all these values in _agg tables for that currency pairs
    """
    def aggregate_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                result = conn.execute(text("SELECT AVG(fxrate) as avg_price, MIN(fxrate) as min_price, MAX(fxrate) as max_price, SUM(cross_number) as total_crosses FROM "+curr[0]+curr[1]+"_raw;"))
                for row in result:
                    avg_price = row.avg_price # mean price for this six minutes data
                    min_price = row.min_price # minimum price for this six minutes data
                    max_price = row.max_price # maximum price for this six minutes data
                    volatility = max_price- min_price
                    print(row.total_crosses)
                    if volatility!=0: # In case if their is no change in currency pair price in 6 minute window
                        fd= row.total_crosses/ volatility # fractal dimension for this six minute data
                    else:
                        fd=0
                curr[2]['upper'].clear() # clear the list to store recent bands value
                curr[2]['lower'].clear() # clear the list to store recent bands value
                self.calculate_keltner_bands(curr, avg_price, volatility) # function will claculate 100 upper bands and 100 lower bands from avg_price for this six minute data and store in corresponding currency pair dictionary. 
                date_res = conn.execute(text("SELECT MAX(ticktime) as last_date FROM "+curr[0]+curr[1]+"_raw;"))
                for row in date_res:
                    last_date = row.last_date
                print({"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd})
                # insert inside the corresponding currency pairs _agg tables
                conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_agg (inserttime, avgfxrate, min_price, max_price, volatility, fractal_dimension) VALUES (:inserttime, :avgfxrate, :min_price, :max_price, :volatility, :fractal_dimension);"),[{"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd}])
    
    """
    # Elements in upper band list are already sorted in ascending order while elements in lower band list are store in decending order.
    # I am using binary search to serch index of larget element smaller than or equal to the current price in upper band and index of smallest element greater than or equal to current price in lower band.
    # If current price is lower than avg price, we search in lower band list, other wise we search for index in upper band list.
    # if index of upper band list is returned, we returned the index as positive number, otherwise we return it as negative.
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

    # this function will fetch data from sqlite tables and create csv files for _agg tables in db file.
    def print_data(self):
        basedir= os.path.abspath(os.path.dirname(__file__))
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                print(curr[0]+curr[1])
                print("------------------------------------------------")
                count_rows= conn.execute(text("SELECT COUNT(*)  FROM "+curr[0]+curr[1]+"_agg;"))
                for row in count_rows:
                    print("total no of rows in table ",row)
                results= pd.read_sql_query("SELECT *  FROM "+curr[0]+curr[1]+"_agg;", self.engine)
                results.to_csv(os.path.join(basedir, curr[0]+curr[1]+".csv"), index= False, sep=";")
                

    """
    This function fetch polygon data every second. We have two counters count and agg_count to check no of hours and no of minutes that our program has run.
    We reset agg_count every six minutes and call
    """
    def keltner_forexdata(self):
        # Number of list iterations - each one should last about 1 second
        count = 0 # counter to keep track of 10 hours execution
        agg_count = 0 # counter to keep track of six minute wndow
        
        # Create the needed tables in the database
        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        
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
                # Set the input variables to the API
                from_ = currency[0]
                to = currency[1]
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
                c_number=0 # # relative no of keltner bands crossed with respect to previous price postion index.
                # after first six minutes we started claculating crosses for current fetch price 
                if count > 360: 
                    # return the index of element smaller than or equal to price from upper band list or greater than or equal to price from lower band list
                    cross_number =self.calculate_crosses(currency, avg_price)  
                    # subtract the previous index from current index to get the current no of bands crossed.
                    # If previous index is in upper band list and current index is in lower band list, we get the addition of upper band index and lower band index as lower band index is retuned in negative and upper band index is returned as positive. 
                    #Simmilarly if both previous and current index in lies in the sams bands i.e upper band list or lower band list, current index will get subtracted from previous index.
                    c_number= cross_number- currency[3]
                    c_number= abs(c_number)
                    currency[3]= cross_number # store index of current price to calculate the relative position of next price
                # Write the data to the SQLite database, raw data tables
                with self.engine.begin() as conn:
                    conn.execute(text("INSERT INTO "+from_+to+"_raw(ticktime, fxrate, inserttime, cross_number) VALUES (:ticktime, :fxrate, :inserttime, :cross_number)"),[{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time, "cross_number": c_number}])



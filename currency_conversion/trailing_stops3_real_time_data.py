import datetime
import time
from polygon import RESTClient
from sqlalchemy import create_engine 
from sqlalchemy import text
from dotenv import dotenv_values
import pandas as pd
import os
import pycaret.regression as pycr
import pycaret.utils as pycu
import numpy as np

# store key value pairs defined in .env file in config
config= dotenv_values(".env")
#pd.set_option('display.float_format', lambda x: '%.3f' % x)

class TrailingStopsData2RealTimeData:
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
        self.currency_pairs = [ ["EUR","USD",{'upper': list(), 'lower': list()},0,100,0],
                                ["GBP","USD",{'upper': list(), 'lower': list()},0,100,0],
                                ["USD","CHF",{'upper': list(), 'lower': list()},0,100,0],
                                ["USD","CAD",{'upper': list(), 'lower': list()},0,100,0],
                                ["USD","HKD",{'upper': list(), 'lower': list()},0,100,0],
                                ["USD","AUD",{'upper': list(), 'lower': list()},0,100,0],
                                ["USD","NZD",{'upper': list(), 'lower': list()},0,100,0],
                                ["USD","SGD",{'upper': list(), 'lower': list()},0,100,0]
                             ]
        # read SQL LITE DB file
        self.engine_old= create_engine("sqlite+pysqlite:///final_trailingstops_2_updated.db", echo=False, future=False)
        # list of training data frames for all the currencies
        self.best_sorting= list()
        with self.engine_old.begin() as conn:
            df = pd.read_sql_query("SELECT best_sort FROM best_classification;", self.engine_old)
            self.best_sorting= df['best_sort'].tolist()
        # API Key
        self.key= config['key']
        # craete SQL LITE DB file
        self.engine= create_engine("sqlite+pysqlite:///final_trailingstops_2_real.db", echo=False, future=False)

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
    return -: fraction of change in current price and price one second before to the price one second before. 
    """
    def initialize_aggregated_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_agg(inserttime text, avgfxrate  numeric, min_price numeric, max_price numeric, volatility numeric, fractal_dimension nuemric, return numeric);"))
    # This will create tables to store predicted returns, actual return and error in prediction for each currency pair
    def initialize_model_performance(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_model_performance(predicted_return  numeric, actual_return  numeric, error  numeric);"))
    
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
        with self.engine.begin() as conn:
            for i, curr in enumerate(self.currency_pairs): 
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
                prev_avg=0 # mean price for previous six minutes. For first six minutes of program execution, prev_agg will be zero.
                agg_data= conn.execute(text("SELECT avgfxrate FROM "+curr[0]+curr[1]+"_agg order by inserttime desc limit 1;")) # get previous six minutes average price.
                for row in agg_data:
                    if len(row) !=0: # For first six minutes of executing program, we will not have any entry in _agg tables. This will check if we have rows in_agg tables or not
                        prev_avg = row.avgfxrate
                if prev_avg !=0: # for first six minutes of program execution, ret will be xero else it will be calculated by below formula.
                    ret= (avg_price-prev_avg)/prev_avg
                else:
                    ret=0
                print({"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd, "return": ret})
                # insert inside the corresponding currency pairs _agg tables
                conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_agg (inserttime, avgfxrate, min_price, max_price, volatility, fractal_dimension, return) VALUES (:inserttime, :avgfxrate, :min_price, :max_price, :volatility, :fractal_dimension, :return);"),[{"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd, "return": ret}])
                details= {"avgfxrate":list(), "volatility": list(), "fractal_dimension": list()}
                details['avgfxrate'].append(avg_price)
                details['volatility'].append( volatility)
                details['fractal_dimension'].append(fd)
                df= pd.DataFrame(details) # create data frame containing current six minutes avgfxrate, volatility, fractal_dimension
                classified_df= pd.DataFrame()
                if self.best_sorting[i]==1: # check if best sorting is sorting_one for this currency pair
                    classified_df=self.clasify_one( curr, df) # classify current vol and fd 
                elif self.best_sorting[i]==2: # check if best sorting is sorting_two for this currency pair
                    classified_df=self.clasify_two( curr, df) # classify current vol and fd
                else: # check if best sorting is sorting_three for this currency pair
                    classified_df=self.clasify_three( curr, df) # classify current vol and fd
                predicted_return= self.predict_real_time_return( curr, classified_df) # predict return for next six minutes
                error= ret- curr[5] # calculated the error by using current actual return and previous predicted return
                print({"predicted_return":curr[5], "actual_return": ret, "error": error})
                # storing the data in table
                conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_model_performance(predicted_return, actual_return , error) VALUES (:predicted_return, :actual_return , :error);"),[{"predicted_return":curr[5], "actual_return": ret, "error": error}])
                curr[5]= predicted_return # saving current predicted return. This will be used in calculating error after next six minutes 

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

    # this function will fetch data from sqlite tables and create csv files for _model_performance tables
    def create_csv(self):
        basedir= os.path.abspath(os.path.dirname(__file__))
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                print(curr[0]+curr[1])
                print("creating csv")
                results= pd.read_sql_query("SELECT *  FROM "+curr[0]+curr[1]+"_model_performance;", self.engine)
                results.to_csv(os.path.join(basedir, curr[0]+curr[1]+"model_perf.csv"), index= False, sep=";")
                

    # It will classify fd and vol in real time if the best sorting method id sorting_one for that currency pair.
    def clasify_one(self, curr, df):
        with self.engine_old.begin() as conn:
            fd_high_min= 0 
            fd_high_max = 0
            fd_medium_min= 0
            fd_medium_max= 0
            fd_low_min= 0
            fd_low_max= 0
            vol_high_min= 0 
            vol_high_max= 0
            vol_medium_min= 0 
            vol_medium_max= 0
            vol_low_min= 0
            vol_low_max= 0
            result= conn.execute(text("SELECT * FROM "+curr[0]+curr[1]+"_sort_one"))
            for row in result:
                fd_high_min= row.fd_high_min
                fd_high_max = row.fd_high_max
                fd_medium_min= row.fd_medium_min
                fd_medium_max= row.fd_medium_max
                fd_low_min= row.fd_low_min
                fd_low_max= row.fd_low_max
                vol_high_min= row.vol_high_min
                vol_high_max= row.vol_high_max
                vol_medium_min= row.vol_medium_min
                vol_medium_max= row.vol_medium_max
                vol_low_min= row.vol_low_min
                vol_low_max= row.vol_low_max
            for j, row in df.iterrows():
                if row['fractal_dimension'] >= fd_high_min and row['fractal_dimension'] <= fd_high_max:
                    row['fractal_dimension'] =1
                elif row['fractal_dimension'] >= fd_medium_min and row['fractal_dimension'] <= fd_medium_max:
                    row['fractal_dimension'] =2
                elif row['fractal_dimension'] >= fd_low_min and row['fractal_dimension'] <= fd_low_max:
                    row['fractal_dimension'] =3
                if row['volatility'] >= vol_high_min and row['volatility'] <= vol_high_max:
                    row['volatility']= 1
                elif row['volatility'] >= vol_medium_min and row['volatility'] <= vol_medium_max:
                    row['volatility']= 2
                elif row['volatility'] >= vol_low_min and row['volatility'] <= vol_low_max:
                    row['volatility']= 3
            return df
    
    # It will classify fd and vol in real time if the best sorting method is sorting_two for that currency pair.
    def clasify_two(self, curr, df):
        with self.engine_old.begin() as conn:
            fd_high_min= 0
            fd_high_max =0
            fd_medium_min = 0
            fd_medium_max = 0
            fd_low_min =0
            fd_low_max = 0
            high_vol_high_min= 0
            high_vol_high_max= 0
            high_vol_medium_min=0
            high_vol_medium_max= 0
            high_vol_low_min= 0
            high_vol_low_max= 0
            medium_vol_high_min=0
            medium_vol_high_max=0
            medium_vol_medium_min= 0
            medium_vol_medium_max=0 
            medium_vol_low_min= 0
            medium_vol_low_max=0
            low_vol_high_min= 0
            low_vol_high_max= 0
            low_vol_medium_min= 0
            low_vol_medium_max= 0
            low_vol_low_min= 0
            low_vol_low_max= 0
            result= conn.execute(text("SELECT * FROM "+curr[0]+curr[1]+"_sort_two"))
            for r in result:
                fd_high_min= r.fd_high_min
                fd_high_max = r.fd_high_max
                fd_medium_min = r.fd_medium_min
                fd_medium_max = r.fd_medium_max
                fd_low_min = r.fd_low_min
                fd_low_max = r.fd_low_max
                high_vol_high_min= r.high_vol_high_min
                high_vol_high_max= r.high_vol_high_max
                high_vol_medium_min= r.high_vol_medium_min
                high_vol_medium_max= r.high_vol_medium_max
                high_vol_low_min= r.high_vol_low_min
                high_vol_low_max= r.high_vol_low_max
                medium_vol_high_min= r.medium_vol_high_min
                medium_vol_high_max= r.medium_vol_high_max
                medium_vol_medium_min= r.medium_vol_medium_min
                medium_vol_medium_max= r.medium_vol_medium_max
                medium_vol_low_min= r.medium_vol_low_min
                medium_vol_low_max= r.medium_vol_low_max
                low_vol_high_min= r.low_vol_high_min
                low_vol_high_max= r.low_vol_high_max
                low_vol_medium_min= r.low_vol_medium_min
                low_vol_medium_max= r.low_vol_medium_max
                low_vol_low_min= r.low_vol_low_min
                low_vol_low_max= r.low_vol_low_max
            for j, row in df.iterrows():
                if row['fractal_dimension'] >= fd_high_min and row['fractal_dimension'] <= fd_high_max:
                    row['fractal_dimension'] =1
                    if row['volatility'] >= high_vol_high_min and row['volatility'] <= high_vol_high_max:
                        row['volatility']= 1
                    elif row['volatility'] >= high_vol_medium_min and row['volatility'] <= high_vol_medium_max:
                        row['volatility']= 2
                    elif row['volatility'] >= high_vol_low_min and row['volatility'] <= high_vol_low_max:
                        row['volatility']= 3
                elif row['fractal_dimension'] >= fd_medium_min and row['fractal_dimension'] <= fd_medium_max:
                    row['fractal_dimension'] =2
                    if row['volatility'] >= medium_vol_high_min and row['volatility'] <= medium_vol_high_max:
                        row['volatility']= 1
                    elif row['volatility'] >= medium_vol_medium_min and row['volatility'] <= medium_vol_medium_max:
                        row['volatility']= 2
                    elif row['volatility'] >= medium_vol_low_min and row['volatility'] <= medium_vol_low_max:
                        row['volatility']= 3
                elif row['fractal_dimension'] >= fd_low_min and row['fractal_dimension'] <= fd_low_max:
                    row['fractal_dimension'] =3
                    if row['volatility'] >= low_vol_high_min and row['volatility'] <= low_vol_high_max:
                        row['volatility']= 1
                    elif row['volatility'] >= low_vol_medium_min and row['volatility'] <= low_vol_medium_max:
                        row['volatility']= 2
                    elif row['volatility'] >= low_vol_low_min and row['volatility'] <= low_vol_low_max:
                        row['volatility']= 3
            return df
    
    # It will classify fd and vol in real time if the best sorting method is sorting_three for that currency pair
    def clasify_three(self, curr, df):
        with self.engine_old.begin() as conn:
            vol_high_min= 0
            vol_high_max= 0
            vol_medium_min= 0
            vol_medium_max=0 
            vol_low_min= 0
            vol_low_max= 0
            high_fd_high_min= 0
            high_fd_high_max= 0
            high_fd_medium_min= 0
            high_fd_medium_max=0 
            high_fd_low_min= 0
            high_fd_low_max= 0
            medium_fd_high_min=0
            medium_fd_high_max= 0
            medium_fd_medium_min= 0
            medium_fd_medium_max=0 
            medium_fd_low_min= 0
            medium_fd_low_max=0
            low_fd_high_min= 0
            low_fd_high_max= 0
            low_fd_medium_min= 0
            low_fd_medium_max=0 
            low_fd_low_min= 0
            low_fd_low_max= 0
            result= conn.execute(text("SELECT * FROM "+curr[0]+curr[1]+"_sort_three"))
            for r in result:
                vol_high_min=  r.vol_high_min
                vol_high_max=  r.vol_high_max
                vol_medium_min= r.vol_medium_min
                vol_medium_max=  r.vol_medium_max
                vol_low_min=  r.vol_low_min
                vol_low_max= r.vol_low_max
                high_fd_high_min= r.high_fd_high_min
                high_fd_high_max= r.high_fd_high_max
                high_fd_medium_min= r.high_fd_medium_min
                high_fd_medium_max= r.high_fd_medium_max
                high_fd_low_min= r.high_fd_low_min
                high_fd_low_max= r.high_fd_low_max
                medium_fd_high_min= r.medium_fd_high_min
                medium_fd_high_max= r.medium_fd_high_max
                medium_fd_medium_min= r.medium_fd_medium_min
                medium_fd_medium_max= r.medium_fd_medium_max
                medium_fd_low_min= r.medium_fd_low_min
                medium_fd_low_max= r.medium_fd_low_max
                low_fd_high_min= r.low_fd_high_min
                low_fd_high_max= r.low_fd_high_max
                low_fd_medium_min= r.low_fd_medium_min
                low_fd_medium_max= r.low_fd_medium_max
                low_fd_low_min= r.low_fd_low_min
                low_fd_low_max= r.low_fd_low_max
            for j, row in df.iterrows():
                if row['volatility'] >= vol_high_min and row['volatility'] <= vol_high_max:
                    row['volatility']=1
                    if row['fractal_dimension'] >= high_fd_high_min and row['fractal_dimension'] <= high_fd_high_max:
                        row['fractal_dimension']=1
                    elif row['fractal_dimension'] >= high_fd_medium_min and row['fractal_dimension'] <= high_fd_medium_max:
                        row['fractal_dimension']=2
                    elif row['fractal_dimension'] >= high_fd_low_min and row['fractal_dimension'] <= high_fd_low_max:
                        row['fractal_dimension']=3
                elif row['volatility'] >= vol_medium_min and row['volatility'] <= vol_medium_max:
                    row['volatility']=2
                    if row['fractal_dimension'] >= medium_fd_high_min and row['fractal_dimension'] <= medium_fd_high_max:
                        row['fractal_dimension']=1
                    elif row['fractal_dimension'] >= medium_fd_medium_min and row['fractal_dimension'] <= medium_fd_medium_max:
                        row['fractal_dimension']=2
                    elif row['fractal_dimension'] >= medium_fd_low_min and row['fractal_dimension'] <= medium_fd_low_max:
                        row['fractal_dimension']=3
                elif row['volatility'] >= vol_low_min and row['volatility'] <= vol_low_max:
                    row['volatility']=3
                    if row['fractal_dimension'] >= low_fd_high_min and row['fractal_dimension'] <= low_fd_high_max:
                        row['fractal_dimension']=1
                    elif row['fractal_dimension'] >= low_fd_medium_min and row['fractal_dimension'] <= low_fd_medium_max:
                        row['fractal_dimension']=2
                    elif row['fractal_dimension'] >= low_fd_low_min and row['fractal_dimension'] <= low_fd_low_max:
                        row['fractal_dimension']=3
            return df
    # it will load the model that is saved for that currency pair and predict the next six return
    def predict_real_time_return(self,curr, df):
        saved_model= pycr.load_model(curr[0]+curr[1]+"_model")
        pred = pycr.predict_model(saved_model, data= df)
        print(pred)
        return pred.iloc[0]['Label']/1000000
            
    """
    This function fetch polygon data every second. We have two counters count and agg_count to check no of hours and no of minutes that our program has run.
    We reset agg_count every six minutes and call 

    """
    def trailing_forexdata2_real_time_data(self):
        # Number of list iterations - each one should last about 1 second
        count = 0 # counter to keep track of 10 hours execution
        agg_count = 0 # counter to keep track of six minute wndow
        
        # Create the needed tables in the database
        self.initialize_raw_data_tables() 
        self.initialize_aggregated_tables()
        self.initialize_model_performance()
        
        # Open a RESTClient for making the api calls
        client= RESTClient(self.key) 
        
        # Loop that runs until the total duration of the program hits 10 hours. 
        while count < 3601: # 3600 seconds = 1 hours
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
                # Write the data to the SQLite database, raw data tables
                with self.engine.begin() as conn:
                    conn.execute(text("INSERT INTO "+from_+to+"_raw(ticktime, fxrate, inserttime, cross_number) VALUES (:ticktime, :fxrate, :inserttime, :cross_number)"),[{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time, "cross_number": c_number}])



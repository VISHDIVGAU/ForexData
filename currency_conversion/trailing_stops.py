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

class TrailingStopsData:
    """
    Fetch Data from polygon API and store in sqllite database
    
    :param key: polygon.io key store in library credentials
    :type key: string

    :param currency_pairs: A dictionary defining the set of currency pairs we will be pulling data for.
    :type currency_pairs: dictionary

    :param count : counter in seconds to check program hits 24 hours.
    :type count:  int

    :param agg_count: counter in seconds to check if 6 minutes has been reached or not 
    :type agg_count: int

    :param engine : Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
    :type engine: sqlalchemy.create_engine
    """

    # Init all the necessary variables when instantiating the class
    def __init__(self):
        self.currency_pairs = [ ["AUD","USD",{'upper': list(), 'lower': list()},0,100],
                                ["EUR","USD",{'upper': list(), 'lower': list()},0,100],
                                ["CAD","USD",{'upper': list(), 'lower': list()},0,100],
                                ["JPY","USD",{'upper': list(), 'lower': list()},0,100],
                                ["CNY","USD",{'upper': list(), 'lower': list()},0,100],
                                ["GBP","USD",{'upper': list(), 'lower': list()},0,100],
                                ["MXN","USD",{'upper': list(), 'lower': list()},0,100],
                                ["CZK","USD",{'upper': list(), 'lower': list()},0,100],
                                ["PLN","USD",{'upper': list(), 'lower': list()},0,100],
                                ["INR","USD",{'upper': list(), 'lower': list()},0,100]
                             ]
        # currency that we sell
        self.short=["GBP","MXN","CZK","PLN","INR"]
        # currency that we bought
        self.long= ["AUD","EUR","CAD","JPY","CNY"]
        # API Key
        self.key= config['key']
        # create/ read SQL LITE DB file
        self.engine= create_engine("sqlite+pysqlite:///final_trailingstops_updated.db", echo=False, future=False)

    # Function slightly modified from polygon sample code to format the date string 
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
    def initialize_aggregated_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_agg(inserttime text, avgfxrate  numeric, min_price numeric, max_price numeric, volatility numeric, fractal_dimension nuemric, return numeric);"))
    # This creates a table for storing the (1 hour interval) aggregated loss or profit data for currency we bought using USD or sell to get USD in the SQLite database
    def initialize_purchase_tables(self):
        with self.engine.begin() as conn:
            for curr in self.long:
                conn.execute(text("CREATE TABLE "+curr+"_bought(inserttime text, initial_investment numeric, agg_return  numeric, profit_loss numeric);"))
            for curr in self.short:
                conn.execute(text("CREATE TABLE "+curr+"_sell(inserttime text, initial_investment numeric, agg_return  numeric, profit_loss numeric);"))

    # function will claculate 100 upper bands and 100 lower bands from avg_price for this six minute data and store in corresponding currency pair dictionary.
    def calculate_keltner_bands(self, curr, avg_price, volatility):
        curr[2]['upper'].append(avg_price)
        curr[2]['lower'].append(avg_price)
        for i in range(100):
            upper_band= avg_price+ (i+1)*0.025*volatility
            lower_band= avg_price- (i+1)*0.025*volatility
            curr[2]['upper'].append(upper_band)
            curr[2]['lower'].append(lower_band)

    # check if total profit or loss is less or greater than 0.25 %, if less increase the investment by 100
    def layer_one(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                if curr[4]!= -1: # check if we stop trading in that currency or not
                    t_return=0
                    p_l=0
                    result = conn.execute(text("SELECT SUM(return) as agg_return FROM "+curr[0]+curr[1]+"_agg order by inserttime desc limit 10;"))
                    for row in result:
                        t_return= curr[4]+ (row.agg_return * curr[4])
                    i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    p_l= t_return- curr[4]
                    pert= (abs(p_l)/curr[4])*100
                    print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l, "pert": pert})
                    if curr[0] in self.long:
                        conn.execute(text("INSERT INTO "+curr[0]+"_bought (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l <= 0 and pert >= 0.25:
                            curr[4]= -1
                        else:
                            curr[4]+= p_l+100
                    if curr[0] in self.short:
                        conn.execute(text("INSERT INTO "+curr[0]+"_sell (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l >= 0 and pert >= 0.25:
                            curr[4]= -1
                        else:
                            curr[4]+= p_l+100

    # check if total profit or loss is less or greater than 0.150 %, if less increase the investment by 100
    def layer_two(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                if curr[4] != -1: # check if we stop trading in that currency or not
                    t_return=0
                    p_l=0
                    result = conn.execute(text("SELECT SUM(return) as agg_return FROM "+curr[0]+curr[1]+"_agg order by inserttime desc limit 10;"))
                    for row in result:
                        t_return= curr[4]+ (row.agg_return * curr[4])
                    i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    p_l= t_return- curr[4]
                    pert= (abs(p_l)/curr[4])*100
                    print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l, "pert": pert})
                    if curr[0] in self.long:
                        conn.execute(text("INSERT INTO "+curr[0]+"_bought (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l <= 0 and pert >= 0.15:
                            curr[4]= -1
                        else:
                            curr[4]+= p_l+100
                    if curr[0] in self.short:
                        conn.execute(text("INSERT INTO "+curr[0]+"_sell (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l >= 0 and pert >= 0.15:
                            curr[4]= -1
                        else:
                            curr[4]+= p_l+100

    # check if total profit or loss is less or greater than 0.10 %, if less increase the investment by 100
    def layer_three(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                if curr[4] != -1: # check if we stop trading in that currency or not
                    t_return=0
                    p_l=0
                    result = conn.execute(text("SELECT SUM(return) as agg_return FROM "+curr[0]+curr[1]+"_agg order by inserttime desc limit 10;"))
                    for row in result:
                        t_return= curr[4]+ (row.agg_return * curr[4])
                    i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    p_l= t_return- curr[4]
                    pert= (abs(p_l)/curr[4])*100
                    print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l, "pert": pert})
                    if curr[0] in self.long:
                        conn.execute(text("INSERT INTO "+curr[0]+"_bought (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l <= 0 and pert >= 0.10:
                            curr[4]= -1
                        else:
                            curr[4]+= p_l+100
                    if curr[0] in self.short:
                        conn.execute(text("INSERT INTO "+curr[0]+"_sell (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l >= 0 and pert >= 0.10:
                            curr[4]= -1
                        else:
                            curr[4]+= p_l+100
    
    # check if total profit or loss is less or greater than 0.05%, if less increase the investment by 100
    def layer_four(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                if curr[4] != -1: # check if we stop trading in that currency or not
                    t_return=0
                    p_l=0
                    result = conn.execute(text("SELECT SUM(return) as agg_return FROM "+curr[0]+curr[1]+"_agg order by inserttime desc limit 10;"))
                    for row in result:
                        t_return= curr[4]+ (row.agg_return * curr[4])
                    i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    p_l= t_return- curr[4]
                    pert= (abs(p_l)/curr[4])*100
                    print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l, "pert": pert})
                    if curr[0] in self.long:
                        conn.execute(text("INSERT INTO "+curr[0]+"_bought (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l <= 0 and pert >= 0.05:
                            curr[4]=-1
                        else:
                            curr[4]+= p_l+100
                    if curr[0] in self.short:
                        conn.execute(text("INSERT INTO "+curr[0]+"_sell (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l >= 0 and pert >= 0.05:
                            curr[4]=-1
                        else:
                            curr[4]+= p_l+100

    # check if total profit or loss is less or greater than 0.05%
    def layer_fifth(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                if curr[4] != -1: # check if we stop trading in that currency or not
                    t_return=0
                    p_l=0
                    result = conn.execute(text("SELECT SUM(return) as agg_return FROM "+curr[0]+curr[1]+"_agg order by inserttime desc limit 10;"))
                    for row in result:
                        t_return= curr[4]+ (row.agg_return * curr[4])
                    i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    p_l= t_return- curr[4]
                    pert= (abs(p_l)/curr[4])*100
                    print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l, "pert": pert})
                    if curr[0] in self.long:
                        conn.execute(text("INSERT INTO "+curr[0]+"_bought (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l <= 0 and pert >= 0.05:
                            curr[4]=-1
                    if curr[0] in self.short:
                        conn.execute(text("INSERT INTO "+curr[0]+"_sell (inserttime, initial_investment, agg_return, profit_loss ) VALUES (:inserttime, :initial_investment, :agg_return, :profit_loss);"),[{"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "profit_loss": p_l}])
                        if p_l >= 0 and pert >= 0.05:
                            curr[4]=-1


    # This function is called every 6 minutes to aggregate the data, store it in the aggregate table, and then delete the raw data
    def aggregate_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                if curr[4] != -1:
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
                    prev_avg=0
                    agg_data= conn.execute(text("SELECT avgfxrate FROM "+curr[0]+curr[1]+"_agg order by inserttime desc limit 1;"))
                    for row in agg_data:
                        if len(row) !=0:
                            prev_avg = row.avgfxrate
                    if prev_avg !=0:
                        ret= (avg_price-prev_avg)/prev_avg
                    else:
                        ret=0
                    print({"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd, "return": ret})
                    # insert inside the corresponding currency pairs _agg tables
                    conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_agg (inserttime, avgfxrate, min_price, max_price, volatility, fractal_dimension, return) VALUES (:inserttime, :avgfxrate, :min_price, :max_price, :volatility, :fractal_dimension, :return);"),[{"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd, "return": ret}])
    
    # Elements in upper band list are already sorted in ascending order while elements in lower band list are store in decending order.
    # I am using binary search to serch index of larget element smaller than or equal to the current price in upper band and index of smallest element greater than or equal to current price in lower band.
    # If current price is lower than avg price, we search in lower band list, other wise we search for index in upper band list.
    # if index of upper band list is returned, we returned the index as positive number, otherwise we return it as negative.
    def calculate_crosses(self, currency, avg_price):
        keltner_Ubands= currency[2]['upper']
        keltner_Lbands=  currency[2]['lower'] # list of 100 upper bands and 100 lower bands from previous six minutes data each of length 101
        length= len(keltner_Ubands)
        #print(length)
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

    # this function will fetch data from sqlite tables and store them in pandas dataframe.
    def create_csv(self):
        basedir= os.path.abspath(os.path.dirname(__file__))
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                print(curr[0]+curr[1])
                print("------------------------------------------------")
                count_rows= conn.execute(text("SELECT COUNT(*)  FROM "+curr[0]+curr[1]+"_agg;"))
                for row in count_rows:
                    print("total no of rows in table ",row)
                    print("creating csv")
                results= pd.read_sql_query("SELECT *  FROM "+curr[0]+curr[1]+"_agg;", self.engine)
                results.to_csv(os.path.join(basedir, curr[0]+curr[1]+".csv"), index= False, sep=";")
                if curr[0] in self.long:
                    res= pd.read_sql_query("SELECT *  FROM "+curr[0]+"_bought;", self.engine)
                    res.to_csv(os.path.join(basedir, curr[0]+"bought.csv"), index= False, sep=";")
                if curr[0] in self.short:
                    res= pd.read_sql_query("SELECT *  FROM "+curr[0]+"_sell;", self.engine)
                    res.to_csv(os.path.join(basedir, curr[0]+"sell.csv"), index= False, sep=";")
    
    # print total investment and profit or loss for each currency pair
    def print_data(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                if curr[0] in self.long:
                    print("currency bought ",curr[0])
                    print("-------------------------------------------------------------")
                    data= conn.execute(text("SELECT initial_investment, agg_return, profit_loss  FROM "+curr[0]+"_bought order by inserttime desc limit 1;"))
                    for row in data:
                        print({"initial_investment": row.initial_investment, "agg_return": row.agg_return, "profit_loss": row.profit_loss})
                if curr[0] in self.short:
                    print("currency sold ",curr[0])
                    print("-------------------------------------------------------------")
                    data= conn.execute(text("SELECT initial_investment, agg_return, profit_loss  FROM "+curr[0]+"_sell order by inserttime desc limit 1;"))
                    for row in data:
                        print({"initial_investment": row.initial_investment, "agg_return": row.agg_return, "profit_loss": row.profit_loss})

    # This function is called every 6 minutes to calucate mean, max, min, volatility(max-min), store it in the aggregate table, and then delete the raw data
    def trailing_forexdata(self):
        # Number of list iterations - each one should last about 1 second
        count = 0
        agg_count = 0
        # Create the needed tables in the database
        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        self.initialize_purchase_tables()
        # Open a RESTClient for making the api calls
        client= RESTClient(self.key) 
        # Loop that runs until the total duration of the program hits 24 hours. 
        while count < 36001: # 36000 seconds = 10 hours
            # check if it reaches one hour
            if count == 3600: # 3600 seconds= 1 hour
                self.layer_one()
            if count == 7200: # 7200 seconds= 2 hours
                self.layer_two()
            if count == 10800: # 10800 seconds = 3 hours
                self.layer_three()
            if count == 14400: # 14400 seconds= 4 hours
                self.layer_four()
            # 18000 second= 5 hours , 21600 seconds= 6 hours, 25200 seconds= 7 hours, 28800 seconds= 8 hours, 32400 seconds= 9 hours and 36000 seconds= 10 hours
            if count == 18000 or count == 21600 or count==25200 or count== 28800 or count== 32400 or count== 36000:
            #if count == 3000 or count== 3600 or count== 4200 or count== 4800 or count== 5400 or count == 6000 or count== 6600 or count== 7200:
                self.layer_fifth()
            # Make a check to see if 6 minutes has been reached or not
            if agg_count == 360:
                # Aggregate the data and clear the raw data tables
                self.aggregate_raw_data_tables()
                self.reset_raw_data_tables()
                agg_count = 0
            # Only call the api every 1 second, so wait here for 0.75 seconds, because the code takes about .15 seconds to run
            time.sleep(0.10)
            # Increment the counters
            count += 1
            agg_count +=1
            # Loop through each currency pair
            for currency in self.currency_pairs:
                # Set the input variables to the API
                if currency[4] != -1:
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
                    cross_number = 0
                    c_number=0
                    # after first six minutes we started claculating crosses for current fetch price 
                    if count > 360: 
                        # return the index of element smaller than or equal to price from upper band list or greater than or equal to price from lower band list
                        cross_number =self.calculate_crosses(currency, avg_price) 
                        #print(cross_number," ",currency[3])
                        c_number= cross_number- currency[3] # subtract the previous index from current index to get the current no of bands crossed.
                        # If previous index is in upper band list and current index is in lower band list, we get the addition of upper band index and lower band index 
                        #as lower band index is retuned in negative and upper band index is returned as positive. 
                        #Simmilarly if both previous and current index in lies in the sams bands i.e upper band list or lower band list, current index will get subtracted from previous index.
                        c_number= abs(c_number)
                        #print("current ",cross_number,"  previous ",currency[3]," c_number ",c_number)
                        currency[3]= cross_number
                    # Write the data to the SQLite database, raw data tables
                    with self.engine.begin() as conn:
                        conn.execute(text("INSERT INTO "+from_+to+"_raw(ticktime, fxrate, inserttime, cross_number) VALUES (:ticktime, :fxrate, :inserttime, :cross_number)"),[{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time, "cross_number": c_number}])
                    #print({"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time, "cross_number": c_number})



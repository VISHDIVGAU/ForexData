import datetime
import time
from polygon import RESTClient
from sqlalchemy import create_engine 
from sqlalchemy import text
from dotenv import dotenv_values
# store key value pairs defined in .env file in config
config= dotenv_values(".env")

class ForexData:
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
        self.currency_pairs = [ ["AUD","USD"],
                                ["GBP","EUR"],
                                ["USD","CAD"],
                                ["USD","JPY"],
                                ["USD","MXN"],
                                ["EUR","USD"],
                                ["USD","CNY"],
                                ["USD","CZK"],
                                ["USD","PLN"],
                                ["USD","INR"]
                             ]
        self.key= config['key']
        self.engine= create_engine("sqlite+pysqlite:///final.db", echo=False, future=True)

    # Function slightly modified from polygon sample code to format the date string 
    def ts_to_datetime(self,ts) -> str:
        return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    # Function which clears the raw data tables once we have aggregated the data in a 6 minute interval
    def reset_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("DROP TABLE "+curr[0]+curr[1]+"_raw;"))
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_raw(ticktime text, fxrate  numeric, inserttime text);"))

    # This creates a table for storing the raw, unaggregated price data for each currency pair in the SQLite database
    def initialize_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_raw(ticktime text, fxrate  numeric, inserttime text);"))

    # This creates a table for storing the (6 min interval) aggregated price data for each currency pair in the SQLite database 
    def initialize_aggregated_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_agg(inserttime text, avgfxrate  numeric, stdfxrate numeric);"))

    # This function is called every 6 minutes to aggregate the data, store it in the aggregate table, and then delete the raw data
    def aggregate_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                result = conn.execute(text("SELECT AVG(fxrate) as avg_price, COUNT(fxrate) as tot_count FROM "+curr[0]+curr[1]+"_raw;"))
                for row in result:
                    avg_price = row.avg_price
                    tot_count = row.tot_count
                std_res = conn.execute(text("SELECT SUM((fxrate - "+str(avg_price)+")*(fxrate - "+str(avg_price)+"))/("+str(tot_count)+"-1) as std_price FROM "+curr[0]+curr[1]+"_raw;"))
                for row in std_res:
                    std_price = sqrt(row.std_price)
                date_res = conn.execute(text("SELECT MAX(ticktime) as last_date FROM "+curr[0]+curr[1]+"_raw;"))
                for row in date_res:
                    last_date = row.last_date
                conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_agg (inserttime, avgfxrate, stdfxrate) VALUES (:inserttime, :avgfxrate, :stdfxrate);"),[{"inserttime": last_date, "avgfxrate": avg_price, "stdfxrate": std_price}])

    def forexdata(self):
        # Number of list iterations - each one should last about 1 second
        count = 0
        agg_count = 0
        # Create the needed tables in the database
        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        # Open a RESTClient for making the api calls
        client= RESTClient(self.key) 
        # Loop that runs until the total duration of the program hits 24 hours. 
        while count < 86400: # 86400 seconds = 24 hours
            # Make a check to see if 6 minutes has been reached or not
            if agg_count == 360:
                # Aggregate the data and clear the raw data tables
                self.aggregate_raw_data_tables()
                self.reset_raw_data_tables()
                agg_count = 0
            # Only call the api every 1 second, so wait here for 0.75 seconds, because the code takes about .15 seconds to run
            time.sleep(0.75)
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
                #print(last_trade.timestamp)
                dt = self.ts_to_datetime(last_trade.timestamp)
                # Get the current time and format it
                insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Calculate the price by taking the average of the bid and ask prices
                avg_price = (last_trade.bid + last_trade.ask)/2
                # Write the data to the SQLite database, raw data tables
                with self.engine.begin() as conn:
                    conn.execute(text("INSERT INTO "+from_+to+"_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"),[{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])
    



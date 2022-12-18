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

class TrailingStopsDataProjectRealTimeData:
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
                                ["USD","SGD",{'upper': list(), 'lower': list()},0,100,0],
                                ["USD","EUR",{'upper': list(), 'lower': list()},0,100,0],
                                ["USD","GBP",{'upper': list(), 'lower': list()},0,100,0],
                                ["CHF","USD",{'upper': list(), 'lower': list()},0,100,0],
                                ["CAD","USD",{'upper': list(), 'lower': list()},0,100,0],
                                ["HKD","USD",{'upper': list(), 'lower': list()},0,100,0],
                                ["AUD","USD",{'upper': list(), 'lower': list()},0,100,0],
                                ["NZD","USD",{'upper': list(), 'lower': list()},0,100,0],
                                ["SGD","USD",{'upper': list(), 'lower': list()},0,100,0]
                             ]
        # currency that we sell
        self.short=["EUR","GBP","CHF","CAD","HKD","AUD","NZD","SGD"]
        # currency that we bought
        self.long= ["EUR","GBP","CHF","CAD","HKD","AUD","NZD","SGD"]
        # create client for mongodb
        self.client = pymongo.MongoClient("127.0.0.1", 27017)
        # database where training data and classification data stored.
        self.db= self.client['ForexData_project']
        # store real time data tables
        self.db_new= self.client['ForexDataProjectRealTime']
        self.best_sorting= list()
        best_coll= self.db['best_classification']
        self.best_sorting= list(best_coll.aggregate([{"$sort":{"_id":1}},{"$project": {"_id":0 ,"best_sort":1}}]))
        # API Key
        self.key= config['key']

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
            collection= self.db_new[curr[0]+curr[1]+"_raw"]
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
    As mentioned in the assignment, this is the first layer where we check if we are making profit or loss. If our loss is greater than 0.25 % in first layer we stop buying or selling that currency further
    else we buy or sell more of that currency.
    To stop further trading in that currency, we change the investment amout to -1 in currency pair list for that currency.

    """
    def layer_one(self):
        for curr in self.currency_pairs:
            sell_coll= self.db_new[curr[0]+curr[1]+"_sell"]
            buy_coll= self.db_new[curr[0]+curr[1]+"_bought"]
            agg_coll= self.db_new[curr[0]+curr[1]+"_agg"]
            t_return=0 # actual agg_return in this one hour window
            p_return=0 # predicted agg_return in next hour
            p_l=0 # actual profit or loss calculated for this one hour window
            pred_p_l=0 # predicted profit or loss in next hour
            result= agg_coll.aggregate([{"$sort": {"_id":-1}},{"$limit":10},{"$group": {"_id": '', "agg_return": { "$sum": "$return" }, "agg_predicted_return": {"$sum": "$future_predicted_return"}}}])
            print(result)
            for row in result:
                t_return= curr[4]+ (row['agg_return'] * curr[4])
                p_return= curr[4]+ (row['agg_predicted_return']*curr[4])
            i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') # current time in which we insert this row in _sell or _bought tables
            p_l= t_return- curr[4]
            pred_p_l= p_return-curr[4]
            error= p_return- t_return
            pert= (abs(error)/curr[4])*100 # profit or loss percentage
            print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
            if curr[1] in self.long :
                buy_coll.insert_one({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
                if (p_l < 0 and pred_p_l > 0) or (p_l >0 and pred_p_l <0):
                    curr[4]+= p_l
                elif p_l <= 0 and pred_p_l <=0 and pert >= 0.25: # for currency that we bought, check if p_l is negtive and % loss is grater than or equal to 0.25. If yes, stop further trading.
                    curr[4]+= p_l
                else:
                    curr[4]+= p_l+100 # if not buy more currency
            if curr[0] in self.short :
                sell_coll.insert_one({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
                if (p_l < 0 and pred_p_l > 0) or (p_l >0 and pred_p_l <0):
                    curr[4]+= p_l
                elif (p_l >= 0 and pred_p_l>=0) and pert >= 0.25: # for currency that we sell, check if p_l is positive and % loss is grater than or equal to 0.25. If yes, stop further trading.
                    curr[4]+= p_l
                else:
                    curr[4]+= p_l+100 # else sell more currency
    
    """
    As mentioned in the assignment, this is the second layer where we check if we are making profit or loss. If our loss is greater than 0.15 % in second layer we stop buying or selling that currency further
    else we buy or sell more of that currency.
    To stop further trading in that currency, we change the investment amout to -1 in currency pair list for that currency.
    """
    def layer_two(self):
        for curr in self.currency_pairs:
            sell_coll= self.db_new[curr[0]+curr[1]+"_sell"]
            buy_coll= self.db_new[curr[0]+curr[1]+"_bought"]
            agg_coll= self.db_new[curr[0]+curr[1]+"_agg"]
           
            t_return=0 # actual agg_return in this one hour window
            p_l=0 # actual profit or loss calculated for this one hour window
            p_return=0 # predicted agg_return of next hour
            pred_p_l=0 # predicted profit or loss in next hour
            
            result= agg_coll.aggregate([{"$sort": {"_id":-1}},{"$limit":10},{"$group": {"_id": '', "agg_return": { "$sum": "$return" }, "agg_predicted_return": {"$sum": "$future_predicted_return"}}}])
            for row in result:
                t_return= curr[4]+ (row['agg_return'] * curr[4])
                p_return= curr[4]+ (row['agg_predicted_return']*curr[4])
            i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') # current time in which we insert this row in _sell or _bought tables
            p_l= t_return- curr[4]
            pred_p_l= p_return-curr[4]
            error= p_return- t_return
            pert= (abs(error)/curr[4])*100  # percentage of profit or loss
            print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return,"agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
            if curr[1] in self.long:
                
                buy_coll.insert_one({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
                if (p_l <0 and pred_p_l >0) or (p_l >0 and pred_p_l <0):
                    curr[4]+= p_l
                elif p_l <= 0 and pred_p_l <=0 and pert >= 0.15: # for currency that we bought, check if p_l is negtive and % loss is grater than or equal to 0.15. If yes, stop further trading.
                    curr[4]+= p_l
                else:
                    curr[4]+= p_l+100 # if not buy more currency
            if curr[0] in self.short :
                
                sell_coll.insert_one({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
                if (p_l <0 and pred_p_l >0) or (p_l >0 and pred_p_l <0):
                    curr[4]+= p_l
                elif p_l >= 0 and pred_p_l >=0 and pert >= 0.15: # for currency that we sell, check if p_l is positive and % loss is grater than or equal to 0.25. If yes, stop further trading.
                    curr[4]+= p_l
                else:
                    curr[4]+= p_l+100  # else sell more currency


    """
    As mentioned in the assignment, this is the third layer where we check if we are making profit or loss. If our loss is greater than 0.10 % in second layer we stop buying or selling that currency further
    else we buy or sell more of that currency.
    To stop further trading in that currency, we change the investment amout to -1 in currency pair list for that currency.
    """
    def layer_three(self):
        
        for curr in self.currency_pairs:
            sell_coll= self.db_new[curr[0]+curr[1]+"_sell"]
            buy_coll= self.db_new[curr[0]+curr[1]+"_bought"]
            agg_coll= self.db_new[curr[0]+curr[1]+"_agg"]
            
            t_return=0 #  actual agg_return in this one hour window
            p_l=0 # actual profit or loss calculated for this one hour window
            p_return=0 # predicted agg_return of next hour
            pred_p_l=0 # predicted profit or loss in next hour
            
            result= agg_coll.aggregate([{"$sort": {"_id":-1}},{"$limit":10},{"$group": {"_id": '', "agg_return": { "$sum": "$return" }, "agg_predicted_return": {"$sum": "$future_predicted_return"}}}])
            for row in result:
                t_return= curr[4]+ (row['agg_return'] * curr[4])
                p_return= curr[4]+ (row['agg_predicted_return']*curr[4])
            i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') # current time in which we insert this row in _sell or _bought tables
            p_l= t_return- curr[4]
            pred_p_l = p_return-curr[4]
            error= p_return- t_return
            pert= (abs(error)/curr[4])*100
            print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert}) 
            if curr[1] in self.long:
                
                buy_coll.insert_one({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
                if (p_l <0 and pred_p_l >0) or (p_l >0 and pred_p_l <0):
                    curr[4]+= p_l
                elif p_l <= 0 and pred_p_l <=0 and pert >= 0.10: # for currency that we bought, check if p_l is negtive and % loss is grater than or equal to 0.10. If yes, stop further trading.
                    curr[4]+= p_l
                else:
                    curr[4]+= p_l+100 # if not buy more currency
            if curr[0] in self.short :
                sell_coll.insert_one({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
                if (p_l <0 and pred_p_l >0) or (p_l >0 and pred_p_l <0):
                    curr[4]+= p_l
                elif p_l >= 0 and pred_p_l >=0 and pert >= 0.10: # for currency that we sell, check if p_l is positive and % loss is grater than or equal to 0.10. If yes, stop further trading.
                    curr[4]+=  p_l
                else:
                    curr[4]+= p_l+100  # else sell more currency
    
    """
    As mentioned in the assignment, this is the fourth layer where we check if we are making profit or loss. If our loss is greater than 0.05 % in second layer we stop buying or selling that currency further
    else we buy or sell more of that currency.
    To stop further trading in that currency, we change the investment amout to -1 in currency pair list for that currency.
    """
    def layer_four(self):
        for curr in self.currency_pairs:
            sell_coll= self.db_new[curr[0]+curr[1]+"_sell"]
            buy_coll= self.db_new[curr[0]+curr[1]+"_bought"]
            agg_coll= self.db_new[curr[0]+curr[1]+"_agg"]
            #if curr[4] != -1: # check if we stop trading in that currency or not
            t_return=0 # actual agg_return in this one hour window
            p_l=0 # actual profit or loss calculated for this one hour window
            p_return=0 # predicted agg_return of next hour
            pred_p_l=0 # predicted profit or loss in next hour
            
            result= agg_coll.aggregate([{"$sort": {"_id":-1}},{"$limit":10},{"$group": {"_id": '', "agg_return": { "$sum": "$return" }, "agg_predicted_return": {"$sum": "$future_predicted_return"}}}])
            for row in result:
                t_return= curr[4]+ (row['agg_return'] * curr[4])
                p_return= curr[4]+ (row['agg_predicted_return']*curr[4])
            i_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') # current time in which we insert this row in _sell or _bought tables
            p_l= t_return- curr[4]
            pred_p_l= p_return- curr[4]
            error= p_return- t_return
            pert= (abs(error)/curr[4])*100
            print({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return,"agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
            if curr[1] in self.long:
                buy_coll.insert_one({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
                if (p_l < 0 and pred_p_l >0) or (p_l >0 and pred_p_l < 0):
                    curr[4]+= p_l
                elif p_l <= 0 and pred_p_l <=0 and pert >= 0.05: # for currency that we bought, check if p_l is negtive and % loss is grater than or equal to 0.05. If yes, stop further trading.
                    curr[4]+= p_l
                else:
                    curr[4]+= p_l+100 # if not buy more currency
            if curr[0] in self.short :
                
                sell_coll.insert_one({"inserttime": i_time, "initial_investment": curr[4], "agg_return": t_return, "agg_predicted_return": p_return, "actual_profit_loss": p_l, "predicted_profit_loss": pred_p_l,"prediction_error": pert})
                if (p_l < 0 and pred_p_l >0) or (p_l >0 and pred_p_l <0):
                    curr[4]+= p_l
                elif p_l >= 0 and pred_p_l >=0 and pert >= 0.05: # for currency that we sell, check if p_l is positive and % loss is grater than or equal to 0.05. If yes, stop further trading.
                    curr[4]+= p_l
                else:
                    curr[4]+= p_l+100 # else sell more currency
    

    """
    This function calculate mean, max, min, volatility, keltner bands, fractal dimension and return for that six minute window.
    we store all these values in _agg tables for that currency pairs.
    """

    def aggregate_raw_data_tables(self):
        
        for i, curr in enumerate(self.currency_pairs):
            collection= self.db_new[curr[0]+curr[1]+"_raw"]
            agg_collection= self.db_new[curr[0]+curr[1]+"_agg"]
            result= collection.aggregate([{"$group": {"_id": '', "avg_price": { "$avg": "$fxrate" }, "min_price": { "$min": "$fxrate" }, "max_price": { "$max": "$fxrate" }, "total_crosses": { "$sum": "$cross_number" },"last_date": { "$max": "$ticktime" }}}])
            
            for row in result:
                avg_price = row['avg_price'] # mean price for this six minutes data
                min_price = row['min_price'] # minimum price for this six minutes data
                max_price = row['max_price'] # maximum price for this six minutes data
                volatility = (max_price- min_price)/avg_price
                last_date = row['last_date']
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
            
            details= {"inserttime":list(),"avgfxrate":list(), "volatility": list(), "fractal_dimension": list()}
            details['inserttime'].append(last_date)
            details['avgfxrate'].append(avg_price)
            details['volatility'].append( volatility)
            details['fractal_dimension'].append(fd)
            df= pd.DataFrame(details) # create data frame containing current six minutes avgfxrate, volatility, fractal_dimension
            df['volatility']= df['volatility'].multiply(10000)
            df['fractal_dimension']= df['fractal_dimension'].divide(1000000)
            df['day']= pd.to_datetime(df['inserttime']).dt.day
            df['hour']= pd.to_datetime(df['inserttime']).dt.hour
            df['minute']= pd.to_datetime(df['inserttime']).dt.minute # next six minutes
            df.loc[df['minute'] >=55, 'hour']= df['hour'].add(1)
            df['minute']= df['minute'].add(6).mod(60)
            df.drop(['inserttime'], axis=1, inplace=True)
            df= df[['day','hour','minute', 'avgfxrate', 'fractal_dimension','volatility']]
            classified_df= pd.DataFrame()
            if self.best_sorting[i]['best_sort'] =='1': # check if best sorting is sorting_one for this currency pair
                classified_df =self.clasify_one(curr, df) # classify current vol and fd
            elif self.best_sorting[i]['best_sort'] =='2': # check if best sorting is sorting_two for this currency pair
                classified_df = self.clasify_two(curr, df) # classify current vol and fd
            else: # check if best sorting is sorting_three for this currency pair
                classified_df = self.clasify_three(curr, df) # classify current vol and fd
            
            predicted_return= self.predict_real_time_return( curr, classified_df) # predict return for next six minutes
            error= ret- curr[5] # calculated the error by using current actual return and previous predicted return
            print({"predicted_return":curr[5], "actual_return": ret, "error": error})
            # storing the data in collection
            agg_collection.insert_one({"inserttime": last_date, "avgfxrate": avg_price, "min_price": min_price, "max_price": max_price, "volatility": volatility, "fractal_dimension": fd, "return": ret, "future_predicted_return": predicted_return, "predicted_return":curr[5], "error": error})
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
    
    # print total investment and profit or loss incurred for each currency pair.
    def print_data(self):
        for curr in self.currency_pairs:
            sell_coll= self.db_new[curr[0]+curr[1]+"_sell"]
            buy_coll= self.db_new[curr[0]+curr[1]+"_bought"]
            if curr[0] in self.long or curr[1] in self.long:
                print(curr[0]+curr[1]+" bought")
                print("--------------------------------------")
                res=buy_coll.find(sort=[( 'inserttime', pymongo.DESCENDING )] ).limit(1)
                for r in res:
                    print(r)
            if curr[0] in self.short or curr[1] in self.short:
                print(curr[0]+curr[1]+" sold")
                print("--------------------------------------")
                res= sell_coll.find(sort=[( 'inserttime', pymongo.DESCENDING )] ).limit(1)
                for r in res:
                    print(r)



    # this function will fetch data from sqlite tables and create csv files for _model_performance tables
    def create_csv(self):
        basedir= os.path.abspath(os.path.dirname(__file__))
        for curr in self.currency_pairs:
            agg_collection= self.db_new[curr[0]+curr[1]+"_agg"]
            sell_coll= self.db_new[curr[0]+curr[1]+"_sell"]
            buy_coll= self.db_new[curr[0]+curr[1]+"_bought"]
            print(curr[0]+curr[1])
            print("--------------------------------------")
            count_rows= agg_collection.aggregate([{"$group":{"_id":'',"total_rows": { "$sum": 1 }}}])
            for row in count_rows:
                print("total no of rows in table ",row['total_rows'])
                print("creating csv")
            results= pd.DataFrame(list(agg_collection.find()))
            results.to_csv(os.path.join(basedir, curr[0]+curr[1]+"_agg_real_time.csv"), index= False, sep=";")
            if curr[0] in self.long or curr[1] in self.long:
                res= pd.DataFrame(list(buy_coll.find()))
                res.to_csv(os.path.join(basedir, curr[0]+curr[1]+"_bought.csv"), index= False, sep=";")
            if curr[0] in self.short or curr[1] in self.short:
                res= pd.DataFrame(list(sell_coll.find()))
                res.to_csv(os.path.join(basedir, curr[0]+curr[1]+"_sell.csv"), index= False, sep=";")


                

    # It will classify fd and vol in real time if the best sorting method id sorting_one for that currency pair.
    def clasify_one(self, curr, df):
        sort_one_coll= self.db[curr[0]+curr[1]+"_sort_one"]
        result= sort_one_coll.find({})
        for row in result:
                fd_high_min= row['fd_high_min']
                fd_high_max = row['fd_high_max']
                fd_medium_min= row['fd_medium_min']
                fd_medium_max= row['fd_medium_max']
                fd_low_min= row['fd_low_min']
                fd_low_max= row['fd_low_max']
                vol_high_min= row['vol_high_min']
                vol_high_max= row['vol_high_max']
                vol_medium_min= row['vol_medium_min']
                vol_medium_max= row['vol_medium_max']
                vol_low_min= row['vol_low_min']
                vol_low_max= row['vol_low_max']
        for j, row in df.iterrows():
            if row['fractal_dimension'] >= fd_high_min and row['fractal_dimension'] <= fd_high_max:
                df.at[j,'fractal_dimension'] =1
            elif row['fractal_dimension'] >= fd_medium_min and row['fractal_dimension'] <= fd_medium_max:
                df.at[j,'fractal_dimension'] =2
            elif row['fractal_dimension'] >= fd_low_min and row['fractal_dimension'] <= fd_low_max:
                df.at[j,'fractal_dimension'] =3
            if row['volatility'] >= vol_high_min and row['volatility'] <= vol_high_max:
                df.at[j,'volatility']= 1
            elif row['volatility'] >= vol_medium_min and row['volatility'] <= vol_medium_max:
                df.at[j,'volatility']= 2
            elif row['volatility'] >= vol_low_min and row['volatility'] <= vol_low_max:
                df.at[j,'volatility']= 3
        return df
    
    # It will classify fd and vol in real time if the best sorting method is sorting_two for that currency pair.
    def clasify_two(self, curr, df):
        sort_two_coll= self.db[curr[0]+curr[1]+"_sort_two"]
        result= sort_two_coll.find()
        for r in result:
            fd_high_min= r['fd_high_min']
            fd_high_max = r['fd_high_max']
            fd_medium_min = r['fd_medium_min']
            fd_medium_max = r['fd_medium_max']
            fd_low_min = r['fd_low_min']
            fd_low_max = r['fd_low_max']
            high_vol_high_min= r['high_vol_high_min']
            high_vol_high_max= r['high_vol_high_max']
            high_vol_medium_min= r['high_vol_medium_min']
            high_vol_medium_max= r['high_vol_medium_max']
            high_vol_low_min= r['high_vol_low_min']
            high_vol_low_max= r['high_vol_low_max']
            medium_vol_high_min= r['medium_vol_high_min']
            medium_vol_high_max= r['medium_vol_high_max']
            medium_vol_medium_min= r['medium_vol_medium_min']
            medium_vol_medium_max= r['medium_vol_medium_max']
            medium_vol_low_min= r['medium_vol_low_min']
            medium_vol_low_max= r['medium_vol_low_max']
            low_vol_high_min= r['low_vol_high_min']
            low_vol_high_max= r['low_vol_high_max']
            low_vol_medium_min= r['low_vol_medium_min']
            low_vol_medium_max= r['low_vol_medium_max']
            low_vol_low_min= r['low_vol_low_min']
            low_vol_low_max= r['low_vol_low_max']
        for j, row in df.iterrows():
            if row['fractal_dimension'] >= fd_high_min and row['fractal_dimension'] <= fd_high_max:
                df.at[j,'fractal_dimension'] =1
                if row['volatility'] >= high_vol_high_min and row['volatility'] <= high_vol_high_max:
                    df.at[j,'volatility']= 1
                elif row['volatility'] >= high_vol_medium_min and row['volatility'] <= high_vol_medium_max:
                    df.at[j,'volatility']= 2
                elif row['volatility'] >= high_vol_low_min and row['volatility'] <= high_vol_low_max:
                    df.at[j,'volatility']= 3
            elif row['fractal_dimension'] >= fd_medium_min and row['fractal_dimension'] <= fd_medium_max:
                df.at[j,'fractal_dimension'] =2
                if row['volatility'] >= medium_vol_high_min and row['volatility'] <= medium_vol_high_max:
                    df.at[j,'volatility']= 1
                elif row['volatility'] >= medium_vol_medium_min and row['volatility'] <= medium_vol_medium_max:
                    df.at[j,'volatility']= 2
                elif row['volatility'] >= medium_vol_low_min and row['volatility'] <= medium_vol_low_max:
                    df.at[j,'volatility']= 3
            elif row['fractal_dimension'] >= fd_low_min and row['fractal_dimension'] <= fd_low_max:
                df.at[j,'fractal_dimension'] =3
                if row['volatility'] >= low_vol_high_min and row['volatility'] <= low_vol_high_max:
                    df.at[j,'volatility']= 1
                elif row['volatility'] >= low_vol_medium_min and row['volatility'] <= low_vol_medium_max:
                    df.at[j,'volatility']= 2
                elif row['volatility'] >= low_vol_low_min and row['volatility'] <= low_vol_low_max:
                    df.at[j,'volatility']= 3
        return df
    
    # It will classify fd and vol in real time if the best sorting method is sorting_three for that currency pair
    def clasify_three(self, curr, df):
        sort_three_coll= self.db[curr[0]+curr[1]+"_sort_three"]
        result= sort_three_coll.find()
        for r in result:
            vol_high_min=  r['vol_high_min']
            vol_high_max=  r['vol_high_max']
            vol_medium_min= r['vol_medium_min']
            vol_medium_max=  r['vol_medium_max']
            vol_low_min=  r['vol_low_min']
            vol_low_max= r['vol_low_max']
            high_fd_high_min= r['high_fd_high_min']
            high_fd_high_max= r['high_fd_high_max']
            high_fd_medium_min= r['high_fd_medium_min']
            high_fd_medium_max= r['high_fd_medium_max']
            high_fd_low_min= r['high_fd_low_min']
            high_fd_low_max= r['high_fd_low_max']
            medium_fd_high_min= r['medium_fd_high_min']
            medium_fd_high_max= r['medium_fd_high_max']
            medium_fd_medium_min= r['medium_fd_medium_min']
            medium_fd_medium_max= r['medium_fd_medium_max']
            medium_fd_low_min= r['medium_fd_low_min']
            medium_fd_low_max= r['medium_fd_low_max']
            low_fd_high_min= r['low_fd_high_min']
            low_fd_high_max= r['low_fd_high_max']
            low_fd_medium_min= r['low_fd_medium_min']
            low_fd_medium_max= r['low_fd_medium_max']
            low_fd_low_min= r['low_fd_low_min']
            low_fd_low_max= r['low_fd_low_max']
        for j, row in df.iterrows():
            if row['volatility'] >= vol_high_min and row['volatility'] <= vol_high_max:
                df.at[j,'volatility']=1
                if row['fractal_dimension'] >= high_fd_high_min and row['fractal_dimension'] <= high_fd_high_max:
                    df.at[j,'fractal_dimension']=1
                elif row['fractal_dimension'] >= high_fd_medium_min and row['fractal_dimension'] <= high_fd_medium_max:
                    df.at[j,'fractal_dimension']=2
                elif row['fractal_dimension'] >= high_fd_low_min and row['fractal_dimension'] <= high_fd_low_max:
                    df.at[j,'fractal_dimension']=3
            elif row['volatility'] >= vol_medium_min and row['volatility'] <= vol_medium_max:
                df.at[j,'volatility']=2
                if row['fractal_dimension'] >= medium_fd_high_min and row['fractal_dimension'] <= medium_fd_high_max:
                    df.at[j,'fractal_dimension']=1
                elif row['fractal_dimension'] >= medium_fd_medium_min and row['fractal_dimension'] <= medium_fd_medium_max:
                    df.at[j,'fractal_dimension']=2
                elif row['fractal_dimension'] >= medium_fd_low_min and row['fractal_dimension'] <= medium_fd_low_max:
                    df.at[j,'fractal_dimension']=3
            elif row['volatility'] >= vol_low_min and row['volatility'] <= vol_low_max:
                df.at[j,'volatility']=3
                if row['fractal_dimension'] >= low_fd_high_min and row['fractal_dimension'] <= low_fd_high_max:
                    df.at[j,'fractal_dimension']=1
                elif row['fractal_dimension'] >= low_fd_medium_min and row['fractal_dimension'] <= low_fd_medium_max:
                    df.at[j,'fractal_dimension']=2
                elif row['fractal_dimension'] >= low_fd_low_min and row['fractal_dimension'] <= low_fd_low_max:
                    df.at[j,'fractal_dimension']=3
        return df
    # it will load the model that is saved for that currency pair and predict the next six return
    def predict_real_time_return(self,curr, df):
        saved_model= pycr.load_model(curr[0]+curr[1]+"_model")
        pred = pycr.predict_model(saved_model, data= df)
        #print(pred)
        return pred.iloc[0]['Label']/1000000
            
    """
    This function fetch polygon data every second. We have two counters count and agg_count to check no of hours and no of minutes that our program has run.
    We reset agg_count every six minutes and call 

    """
    def trailing_forexdata_project_real_time_data(self):
        # Number of list iterations - each one should last about 1 second
        count = 0 # counter to keep track of 10 hours execution
        agg_count = 0 # counter to keep track of six minute wndow
        
        # Open a RESTClient for making the api calls
        client= RESTClient(self.key) 
        
        # Loop that runs until the total duration of the program hits 10 hours. 
        while count < 36001: # 3600 seconds = 1 hours
            # check if program is executed for one hour
            if count == 3600: # 3600 seconds= 1 hour
                self.layer_one() # check the profit or loss percentage is less or grater than 0.25 %
            # check if program is executed for two hour
            if count == 7200: # 7200 seconds= 2 hours
                self.layer_two() # check the profit or loss percentage is less or grater than 0.15 %
            # check if program is executed for three hour
            if count == 10800: # 10800 seconds = 3 hours
                self.layer_three() # check the profit or loss percentage is less or grater than 0.10 %
            # call layer_four() function every hour henceforth till program is executed for 10 hours
            # 14400 seconds= 4 hours, 18000 second= 5 hours , 21600 seconds= 6 hours, 25200 seconds= 7 hours, 28800 seconds= 8 hours, 32400 seconds= 9 hours and 36000 seconds= 10 hours
            if count== 14400 or count == 18000 or count == 21600 or count==25200 or count== 28800 or count== 32400 or count== 36000:
                self.layer_four() # check the profit or loss percentage is less or grater than 0.05 %
            # check if program is executed for six minutes, if yes, call function aggregate_raw_data_tables() and reset_raw_data_tables() and reset agg_count to zero
            if agg_count == 360:
                # Aggregate the data and clear the raw data tables
                self.aggregate_raw_data_tables()
                self.reset_raw_data_tables()
                agg_count = 0
            time.sleep(0.30)
            # Increment the counters
            count += 1
            agg_count +=1
            # Loop through each currency pair
            for currency in self.currency_pairs:
                from_ = currency[0] 
                to = currency[1]
                collection= self.db_new[from_+to+"_raw"]
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
                # insert in _raw collection
                collection.insert_one({"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time, "cross_number": c_number})


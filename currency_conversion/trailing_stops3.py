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

class TrailingStopsData3:
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
        self.currency_pairs = [ ["EUR","USD",{'upper': list(), 'lower': list()},0,100,
                                {'sorting_one':{'fractal_dimension':{'high':[],'medium':[],'low':[]},'volatility':{'high':[],'medium':[],'low':[]}},
                                    'sorting_two':{'fractal_dimension':{'high':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}},'low':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}}},
                                'sorting_three':{'volatility':{'high':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'low':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}}}
                                    }],
                                ["GBP","USD",{'upper': list(), 'lower': list()},0,100,
                                {'sorting_one':{'fractal_dimension':{'high':[],'medium':[],'low':[]},'volatility':{'high':[],'medium':[],'low':[]}},
                                'sorting_two':{'fractal_dimension':{'high':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}},'low':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}}},
                                'sorting_three':{'volatility':{'high':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'low':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}}}
                                    }],
                                ["USD","CHF",{'upper': list(), 'lower': list()},0,100,
                                {'sorting_one':{'fractal_dimension':{'high':[],'medium':[],'low':[]},'volatility':{'high':[],'medium':[],'low':[]}},
                                'sorting_two':{'fractal_dimension':{'high':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}},'low':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}}},
                                'sorting_three':{'volatility':{'high':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'low':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}}}
                                    }],
                                ["USD","CAD",{'upper': list(), 'lower': list()},0,100,
                                {'sorting_one':{'fractal_dimension':{'high':[],'medium':[],'low':[]},'volatility':{'high':[],'medium':[],'low':[]}},
                                'sorting_two':{'fractal_dimension':{'high':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}},'low':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}}},
                                'sorting_three':{'volatility':{'high':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'low':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}}}
                                    }],
                                ["USD","HKD",{'upper': list(), 'lower': list()},0,100,
                                {'sorting_one':{'fractal_dimension':{'high':[],'medium':[],'low':[]},'volatility':{'high':[],'medium':[],'low':[]}},
                                'sorting_two':{'fractal_dimension':{'high':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}},'low':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}}},
                                'sorting_three':{'volatility':{'high':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'low':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}}}
                                    }],
                                ["USD","AUD",{'upper': list(), 'lower': list()},0,100,
                                {'sorting_one':{'fractal_dimension':{'high':[],'medium':[],'low':[]},'volatility':{'high':[],'medium':[],'low':[]}},
                                'sorting_two':{'fractal_dimension':{'high':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}},'low':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}}},
                                'sorting_three':{'volatility':{'high':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'low':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}}}
                                    }],
                                ["USD","NZD",{'upper': list(), 'lower': list()},0,100,
                                {'sorting_one':{'fractal_dimension':{'high':[],'medium':[],'low':[]},'volatility':{'high':[],'medium':[],'low':[]}},
                                'sorting_two':{'fractal_dimension':{'high':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}},'low':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}}},
                                'sorting_three':{'volatility':{'high':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'low':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}}}
                                    }],
                                ["USD","SGD",{'upper': list(), 'lower': list()},0,100,
                                {'sorting_one':{'fractal_dimension':{'high':[],'medium':[],'low':[]},'volatility':{'high':[],'medium':[],'low':[]}},
                                'sorting_two':{'fractal_dimension':{'high':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}},'low':{'range':[], 'volatility':{'high':[],'medium':[],'low':[]}}}},
                                'sorting_three':{'volatility':{'high':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'medium':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}, 'low':{'range':[], 'fractal_dimension':{'high':[],'medium':[],'low':[]}}}}
                                    }]
                             ]
        # list of training data frames for all the currencies
        self.dfs=list()
        # list of prediction
        self.predictions= list()
        # API Key
        self.key= config['key']
        # create the client for mongodb which connect to  mongo server in our case localhost. 
        self.client = pymongo.MongoClient("127.0.0.1", 27017)
        # it will create / use datbase in mongodb.
        self.db= self.client['ForexData']


    # Function which clears the all the tables. This funcion has to be used only when we starting training again. 
    def reset_tables(self):
            collection= self.db['best_classification']
            collection.delete_many({})
            for curr in self.currency_pairs:
                coll_one= self.db[curr[0]+curr[1]+"_sort_one"]
                coll_one.delete_many({})
                coll_two= self.db[curr[0]+curr[1]+"_sort_two"]
                coll_two.delete_many({})
                coll_three= self.db[curr[0]+curr[1]+"_sort_three"]
                coll_three.delete_many({})
    
    """
    #this function find min and max ranges for three partions after fd and volatility columns get sorted independently.
    # we will get total 3 paird of ranges for each FD and Volatility.
    # FD and Volatility are being sorted in decreasing order.
    # fd and volatility are partioned as [0-32] high, [33-66] medium, [67-100] low.
    """
    def sorting_one(self):
        for curr in self.currency_pairs:
            agg_coll= self.db[curr[0]+curr[1]+"_agg"]
            sort_one_coll= self.db[curr[0]+curr[1]+"_sort_one"]
            df_fd= pd.DataFrame(list(agg_coll.aggregate([{"$project": {"_id":0 ,"fractal_dimension":1}}])))
            df_vol= pd.DataFrame(list(agg_coll.aggregate([{"$project": {"_id":0 ,"volatility":1}}])))    
            df_fd= df_fd.sort_values(by=['fractal_dimension'], ascending= [False]) # sorting in decending order
            df_vol= df_vol.sort_values(by=['volatility'], ascending= [False]) # sorting in decending order
            hfd= df_fd.iloc[:33,:].agg(['min', 'max']) # first 33 elements in FD
            hvol= df_vol.iloc[:33,:].agg(['min', 'max']) # first 33 elements in volatility
            curr[5]['sorting_one']['fractal_dimension']['high'].extend(hfd.values.tolist()) # saving ranges in dictionary for this currency in currency pair list
            curr[5]['sorting_one']['volatility']['high'].extend(hvol.values.tolist()) #saving ranges in dictionary for this currency in currency pair list
            mfd= df_fd.iloc[33:67,:].agg(['min', 'max']) # next 32 elements in fd
            mvol= df_vol.iloc[33:67,:].agg(['min', 'max']) # next 32 elemenst in Volatility
            curr[5]['sorting_one']['fractal_dimension']['medium'].extend(mfd.values.tolist()) 
            curr[5]['sorting_one']['volatility']['medium'].extend(mvol.values.tolist()) 
            lfd= df_fd.iloc[67:,:].agg(['min', 'max']) # next 33 elements of fd
            lvol=df_vol.iloc[67:,:].agg(['min', 'max']) # next 33 elements of volatility
            curr[5]['sorting_one']['fractal_dimension']['low'].extend(lfd.values.tolist())
            curr[5]['sorting_one']['volatility']['low'].extend(lvol.values.tolist())
            # variable to be use in storing data in database
            fd_high_min= curr[5]['sorting_one']['fractal_dimension']['high'][0][0]
            fd_high_max = curr[5]['sorting_one']['fractal_dimension']['high'][1][0]
            fd_medium_min= curr[5]['sorting_one']['fractal_dimension']['medium'][0][0]
            fd_medium_max= curr[5]['sorting_one']['fractal_dimension']['medium'][1][0]
            fd_low_min= curr[5]['sorting_one']['fractal_dimension']['low'][0][0]
            fd_low_max= curr[5]['sorting_one']['fractal_dimension']['low'][1][0]
            vol_high_min= curr[5]['sorting_one']['volatility']['high'][0][0]
            vol_high_max= curr[5]['sorting_one']['volatility']['high'][1][0]
            vol_medium_min= curr[5]['sorting_one']['volatility']['medium'][0][0]
            vol_medium_max= curr[5]['sorting_one']['volatility']['medium'][1][0]
            vol_low_min= curr[5]['sorting_one']['volatility']['low'][0][0]
            vol_low_max= curr[5]['sorting_one']['volatility']['low'][1][0]
            # it will store the values in sorting on table for this currency. Tables will come in use when we classify real time data.
            sort_one_coll.insert_one({"fd_high_min":fd_high_min, "fd_high_max":fd_high_max, "fd_medium_min":fd_medium_min, "fd_medium_max":fd_medium_max, "fd_low_min":fd_low_min, "fd_low_max":fd_low_max, "vol_high_min":vol_high_min, "vol_high_max":vol_high_max, "vol_medium_min":vol_medium_min, "vol_medium_max":vol_medium_max, "vol_low_min":vol_low_min, "vol_low_max":vol_low_max})

    """
    #this function find 3 pairs min and max ranges for three partions of fd coloumn first after being sorted first.
    #then for individual partion, we will sort volatility coloumn and partion it in three. 
    # thus for each partion, we will have three pairs of ranges for volatility.
    # FD and Volatility are being sorted in decreasing order.
    # FD coloumn is partioned as [0-32] high, [33-66] medium, [67-100] low.
    # Volatility coloumn is partioned as [0-10] high, [11-21] medium and [22-32] low for each fd partion. 
    """
    def sorting_two(self):
        for curr in self.currency_pairs:
            agg_coll= self.db[curr[0]+curr[1]+"_agg"]
            sort_two_coll= self.db[curr[0]+curr[1]+"_sort_two"]
            df= pd.DataFrame(list(agg_coll.aggregate([{"$project": {"_id":0 ,"fractal_dimension":1,"volatility":1}}])))    
            df_fd= df.sort_values(by=['fractal_dimension'], ascending= [False]) # sort in decending order by fd
            hfd= df_fd.iloc[:33,:].agg({'fractal_dimension':['min', 'max']}) # first 32 elements of frame
            hvol= df_fd.iloc[:33,:].sort_values(by=['volatility'], ascending= [False]) #  sort the partion frame by volatility
            hvol= hvol.reset_index(drop=True) # reset the index
            hvolh= hvol.iloc[:11,:].agg({'volatility':['min', 'max']}) # first 11 elements of frame
            hvolm= hvol.iloc[11:22,:].agg({'volatility':['min', 'max']}) # next 11 elements of frame
            hvoll= hvol.iloc[22:,:].agg({'volatility':['min', 'max']}) # next 11 elements of frame
            curr[5]['sorting_two']['fractal_dimension']['high']['range'].extend(hfd['fractal_dimension'].tolist()) #  saving ranges in dictionary for this currency in currency pair list
            curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['high'].extend(hvolh['volatility'].tolist()) # saving ranges in dictionary for this currency in currency pair list
            curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['medium'].extend(hvolm['volatility'].tolist()) # saving ranges in dictionary for this currency in currency pair list
            curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['low'].extend(hvoll['volatility'].tolist()) # saving ranges in dictionary for this currency in currency pair list
            mfd= df_fd.iloc[33:67,:].agg({'fractal_dimension':['min', 'max']}) 
            mvol= df_fd.iloc[33:67,:].sort_values(by=['volatility'], ascending= [False])
            mvol= mvol.reset_index(drop=True)
            mvolh= mvol.iloc[:11,:].agg({'volatility':['min', 'max']})
            mvolm= mvol.iloc[11:22,:].agg({'volatility':['min', 'max']})
            mvoll= mvol.iloc[22:,:].agg({'volatility':['min', 'max']})
            curr[5]['sorting_two']['fractal_dimension']['medium']['range'].extend(mfd['fractal_dimension'].tolist())
            curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['high'].extend(mvolh['volatility'].tolist())
            curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['medium'].extend(mvolm['volatility'].tolist())
            curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['low'].extend(mvoll['volatility'].tolist())
            lfd= df_fd.iloc[67:,:].agg({'fractal_dimension':['min', 'max']})
            lvol= df_fd.iloc[67:,:].sort_values(by=['volatility'], ascending= [False])
            lvol= lvol.reset_index(drop=True)
            lvolh= lvol.iloc[:11,:].agg({'volatility':['min', 'max']})
            lvolm= lvol.iloc[11:22,:].agg({'volatility':['min', 'max']})
            lvoll= lvol.iloc[22:,:].agg({'volatility':['min', 'max']})
            curr[5]['sorting_two']['fractal_dimension']['low']['range'].extend(lfd['fractal_dimension'].tolist())
            curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['high'].extend(lvolh['volatility'].tolist())
            curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['medium'].extend(lvolm['volatility'].tolist())
            curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['low'].extend(lvoll['volatility'].tolist())
            # variables use to store data in database
            fd_high_min= curr[5]['sorting_two']['fractal_dimension']['high']['range'][0]
            fd_high_max = curr[5]['sorting_two']['fractal_dimension']['high']['range'][1]
            fd_medium_min = curr[5]['sorting_two']['fractal_dimension']['medium']['range'][0]
            fd_medium_max = curr[5]['sorting_two']['fractal_dimension']['medium']['range'][1]
            fd_low_min = curr[5]['sorting_two']['fractal_dimension']['low']['range'][0]
            fd_low_max = curr[5]['sorting_two']['fractal_dimension']['low']['range'][1]
            high_vol_high_min= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['high'][0]
            high_vol_high_max= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['high'][1]
            high_vol_medium_min= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['medium'][0]
            high_vol_medium_max= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['medium'][1]
            high_vol_low_min= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['low'][0]
            high_vol_low_max= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['low'][1]
            medium_vol_high_min= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['high'][0]
            medium_vol_high_max= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['high'][1]
            medium_vol_medium_min= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['medium'][0]
            medium_vol_medium_max= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['medium'][1]
            medium_vol_low_min= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['low'][0]
            medium_vol_low_max= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['low'][1]
            low_vol_high_min= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['high'][0]
            low_vol_high_max= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['high'][1]
            low_vol_medium_min= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['medium'][0]
            low_vol_medium_max= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['medium'][1]
            low_vol_low_min= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['low'][0]
            low_vol_low_max= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['low'][1]
            # this will store data in sort_two table. We will use the table when we classify real time data.
            sort_two_coll.insert_one({"fd_high_min":fd_high_min, "fd_high_max":fd_high_max, "fd_medium_min":fd_medium_min, "fd_medium_max":fd_medium_max, "fd_low_min":fd_low_min, "fd_low_max":fd_low_max, "high_vol_high_min":high_vol_high_min, "high_vol_high_max":high_vol_high_max, "high_vol_medium_min":high_vol_medium_min, "high_vol_medium_max":high_vol_medium_max, "high_vol_low_min":high_vol_low_min, "high_vol_low_max":high_vol_low_max, "medium_vol_high_min":medium_vol_high_min, "medium_vol_high_max":medium_vol_high_max, "medium_vol_medium_min":medium_vol_medium_min, "medium_vol_medium_max":medium_vol_medium_max, "medium_vol_low_min":medium_vol_low_min, "medium_vol_low_max":medium_vol_low_max, "low_vol_high_min":low_vol_high_min, "low_vol_high_max":low_vol_high_max, "low_vol_medium_min":low_vol_medium_min, "low_vol_medium_max":low_vol_medium_max, "low_vol_low_min":low_vol_low_min, "low_vol_low_max":low_vol_low_max})
    
    """
    #this function find 3 pairs min and max ranges for three partions of volatility coloumn first after being sorted first.
    #then for individual partion, we will sort fd coloumn and partion it in three. 
    # thus for each partion, we will have three pairs of ranges for fd.
    # FD and Volatility are being sorted in decreasing order.
    # volatility coloumn is partioned as [0-32] high, [33-66] medium, [67-100] low.
    # fd coloumn is partioned as [0-10] high, [11-21] medium and [22-32] low for each fd partion. 
    """
    def sorting_three(self):
        for curr in self.currency_pairs:
            agg_coll= self.db[curr[0]+curr[1]+"_agg"]
            sort_three_coll= self.db[curr[0]+curr[1]+"_sort_three"]
            df= pd.DataFrame(list(agg_coll.aggregate([{"$project": {"_id":0 ,"fractal_dimension":1,"volatility":1}}])))
            df_vol= df.sort_values(by=['volatility'], ascending= [False]) # sort the frame in decending order by volatility
            hvol= df_vol.iloc[:33,:].agg({'volatility':['min', 'max']}) # first 33 elements in the sorted frame
            hfd= df_vol.iloc[:33,:].sort_values(by=['fractal_dimension'], ascending= [False]) # sort the partion in decending order by  fd
            hfd= hfd.reset_index(drop=True) # reset the index
            hfdh= hfd.iloc[:11,:].agg({'fractal_dimension':['min', 'max']}) # first 11 elements in sorted frame
            hfdm= hfd.iloc[11:22,:].agg({'fractal_dimension':['min', 'max']}) # next 11 elements in sorted frame
            hfdl= hfd.iloc[22:,:].agg({'fractal_dimension':['min', 'max']}) # next 11 elements in sorted framee
            curr[5]['sorting_three']['volatility']['high']['range'].extend(hvol['volatility'].tolist()) # saving ranges in dictionary for this currency in currency pair list
            curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['high'].extend(hfdh['fractal_dimension'].tolist()) # saving ranges in dictionary for this currency in currency pair list
            curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['medium'].extend(hfdm['fractal_dimension'].tolist()) # saving ranges in dictionary for this currency in currency pair list
            curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['low'].extend(hfdl['fractal_dimension'].tolist()) # saving ranges in dictionary for this currency in currency pair list
            mvol= df_vol.iloc[33:67,:].agg({'volatility':['min', 'max']})
            mfd= df_vol.iloc[33:67,:].sort_values(by=['fractal_dimension'], ascending= [False])
            mfd= mfd.reset_index(drop=True)
            mfdh= mfd.iloc[:11,:].agg({'fractal_dimension':['min', 'max']})
            mfdm= mfd.iloc[11:22,:].agg({'fractal_dimension':['min', 'max']})
            mfdl= mfd.iloc[22:,:].agg({'fractal_dimension':['min', 'max']})
            curr[5]['sorting_three']['volatility']['medium']['range'].extend(mvol['volatility'].tolist())
            curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['high'].extend(mfdh['fractal_dimension'].tolist())
            curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['medium'].extend(mfdm['fractal_dimension'].tolist())
            curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['low'].extend(mfdl['fractal_dimension'].tolist())
            lvol= df_vol.iloc[67:,:].agg({'volatility':['min', 'max']})
            lfd= df_vol.iloc[67:,:].sort_values(by=['fractal_dimension'], ascending= [False])
            lfd= lfd.reset_index(drop=True)
            lfdh= lfd.iloc[:11,:].agg({'fractal_dimension':['min', 'max']})
            lfdm= lfd.iloc[11:22,:].agg({'fractal_dimension':['min', 'max']})
            lfdl= lfd.iloc[22:,:].agg({'fractal_dimension':['min', 'max']})
            curr[5]['sorting_three']['volatility']['low']['range'].extend(lvol['volatility'].tolist())
            curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['high'].extend(lfdh['fractal_dimension'].tolist())
            curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['medium'].extend(lfdm['fractal_dimension'].tolist())
            curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['low'].extend(lfdl['fractal_dimension'].tolist())
            # variables use to store data in database
            vol_high_min= curr[5]['sorting_three']['volatility']['high']['range'][0]
            vol_high_max= curr[5]['sorting_three']['volatility']['high']['range'][1]
            vol_medium_min= curr[5]['sorting_three']['volatility']['medium']['range'][0]
            vol_medium_max= curr[5]['sorting_three']['volatility']['medium']['range'][1]
            vol_low_min= curr[5]['sorting_three']['volatility']['low']['range'][0]
            vol_low_max= curr[5]['sorting_three']['volatility']['low']['range'][1]
            high_fd_high_min= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['high'][0]
            high_fd_high_max= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['high'][1]
            high_fd_medium_min= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['medium'][0]
            high_fd_medium_max= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['medium'][1]
            high_fd_low_min= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['low'][0]
            high_fd_low_max= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['low'][1]
            medium_fd_high_min= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['high'][0]
            medium_fd_high_max= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['high'][1]
            medium_fd_medium_min= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['medium'][0]
            medium_fd_medium_max= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['medium'][1]
            medium_fd_low_min= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['low'][0]
            medium_fd_low_max= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['low'][1]
            low_fd_high_min= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['high'][0]
            low_fd_high_max= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['high'][1]
            low_fd_medium_min= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['medium'][0]
            low_fd_medium_max= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['medium'][1]
            low_fd_low_min= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['low'][0]
            low_fd_low_max= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['low'][1]
            # it will store data in sort_three table. This will be used to classify data in real time
            sort_three_coll.insert_one({"vol_high_min":vol_high_min, "vol_high_max":vol_high_max, "vol_medium_min":vol_medium_min, "vol_medium_max":vol_medium_max, "vol_low_min":vol_low_min, "vol_low_max":vol_low_max, "high_fd_high_min":high_fd_high_min, "high_fd_high_max":high_fd_high_max, "high_fd_medium_min":high_fd_medium_min, "high_fd_medium_max":high_fd_medium_max, "high_fd_low_min":high_fd_low_min, "high_fd_low_max":high_fd_low_max, "medium_fd_high_min":medium_fd_high_min, "medium_fd_high_max":medium_fd_high_max, "medium_fd_medium_min":medium_fd_medium_min, "medium_fd_medium_max":medium_fd_medium_max, "medium_fd_low_min":medium_fd_low_min, "medium_fd_low_max":medium_fd_low_max, "low_fd_high_min":low_fd_high_min, "low_fd_high_max":low_fd_high_max, "low_fd_medium_min":low_fd_medium_min,"low_fd_medium_max":low_fd_medium_max, "low_fd_low_min":low_fd_low_min, "low_fd_low_max":low_fd_low_max})

    # For every currency pair, it class classifing menthods for all three sorting methods and store the classified data frames in self.dfs.
    def clasify(self):
        sorting_list= ["sorting_one","sorting_two","sorting_three"]
        for i, curr in enumerate(self.currency_pairs):
            self.dfs.append([])
            for sort_m in sorting_list:
                if sort_m == "sorting_one":
                    self.clasify_one(curr, i)
                if sort_m =="sorting_two":
                    self.clasify_two(curr, i)
                if sort_m =="sorting_three":
                    self.clasify_three(curr, i)
    
    # classify fd and vol as per ranges we got from sorting_one fnction.
    def clasify_one(self, curr, i):
        agg_coll= self.db[curr[0]+curr[1]+"_agg"]
        df= pd.DataFrame(list(agg_coll.aggregate([{"$project": {"_id":0 ,"avgfxrate":1,"fractal_dimension":1,"volatility":1,"return":1}}])))
        df['return']= df['return'].multiply(1000000)
        high_fd= curr[5]['sorting_one']['fractal_dimension']['high']
        high_vol= curr[5]['sorting_one']['volatility']['high']
        medium_fd= curr[5]['sorting_one']['fractal_dimension']['medium']
        medium_vol= curr[5]['sorting_one']['volatility']['medium']
        low_fd= curr[5]['sorting_one']['fractal_dimension']['low']
        low_vol= curr[5]['sorting_one']['volatility']['low']
        for j, row in df.iterrows():
            if row['fractal_dimension'] >= high_fd[0] and row['fractal_dimension'] <= high_fd[1]:
                row['fractal_dimension'] =1
            elif row['fractal_dimension'] >= medium_fd[0] and row['fractal_dimension'] <= medium_fd[1]:
                row['fractal_dimension'] =2
            elif row['fractal_dimension'] >= low_fd[0] and row['fractal_dimension'] <= low_fd[1]:
                row['fractal_dimension'] =3
            if row['volatility'] >= high_vol[0] and row['volatility'] <= high_vol[1]:
                row['volatility']= 1
            elif row['volatility'] >= medium_vol[0] and row['volatility'] <= medium_vol[1]:
                row['volatility']= 2
            elif row['volatility'] >= low_vol[0] and row['volatility'] <= low_vol[1]:
                row['volatility']= 3
        self.dfs[i].append(df)
    
    # classify fd and vol as per ranges we got from sorting_two fnction.
    def clasify_two(self, curr, i):
        agg_coll= self.db[curr[0]+curr[1]+"_agg"]
        df= pd.DataFrame(list(agg_coll.aggregate([{"$project": {"_id":0 ,"avgfxrate":1,"fractal_dimension":1,"volatility":1,"return":1}}])))
        df['return']= df['return'].multiply(1000000)
        high_fd= curr[5]['sorting_two']['fractal_dimension']['high']['range']
        high_vol_h= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['high']
        high_vol_m= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['medium']
        high_vol_l= curr[5]['sorting_two']['fractal_dimension']['high']['volatility']['low']
        medium_fd= curr[5]['sorting_two']['fractal_dimension']['medium']['range']
        medium_vol_h= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['high']
        medium_vol_m= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['medium']
        medium_vol_l= curr[5]['sorting_two']['fractal_dimension']['medium']['volatility']['low']
        low_fd= curr[5]['sorting_two']['fractal_dimension']['low']['range']
        low_vol_h= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['high']
        low_vol_m= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['medium']
        low_vol_l= curr[5]['sorting_two']['fractal_dimension']['low']['volatility']['low']
        for j, row in df.iterrows():
            if row['fractal_dimension'] >= high_fd[0] and row['fractal_dimension'] <= high_fd[1]:
                row['fractal_dimension'] =1
                if row['volatility'] >= high_vol_h[0] and row['volatility'] <= high_vol_h[1]:
                    row['volatility']= 1
                elif row['volatility'] >= high_vol_m[0] and row['volatility'] <= high_vol_m[1]:
                    row['volatility']= 2
                elif row['volatility'] >= high_vol_l[0] and row['volatility'] <= high_vol_l[1]:
                    row['volatility']= 3
            elif row['fractal_dimension'] >= medium_fd[0] and row['fractal_dimension'] <= medium_fd[1]:
                row['fractal_dimension'] =2
                if row['volatility'] >= medium_vol_h[0] and row['volatility'] <= medium_vol_h[1]:
                    row['volatility']= 1
                elif row['volatility'] >= medium_vol_m[0] and row['volatility'] <= medium_vol_m[1]:
                    row['volatility']= 2
                elif row['volatility'] >= medium_vol_l[0] and row['volatility'] <= medium_vol_l[1]:
                    row['volatility']= 3
            elif row['fractal_dimension'] >= low_fd[0] and row['fractal_dimension'] <= low_fd[1]:
                row['fractal_dimension'] =3
                if row['volatility'] >= low_vol_h[0] and row['volatility'] <= low_vol_h[1]:
                    row['volatility']= 1
                elif row['volatility'] >= low_vol_m[0] and row['volatility'] <= low_vol_m[1]:
                    row['volatility']= 2
                elif row['volatility'] >= low_vol_l[0] and row['volatility'] <= low_vol_l[1]:
                    row['volatility']= 3
        self.dfs[i].append(df)

    #classify fd and vol as per ranges we got from sorting_three fnction.
    def clasify_three(self, curr, i):
        agg_coll= self.db[curr[0]+curr[1]+"_agg"]
        df= pd.DataFrame(list(agg_coll.aggregate([{"$project": {"_id":0 ,"avgfxrate":1,"fractal_dimension":1,"volatility":1,"return":1}}])))
        df['return']= df['return'].multiply(1000000)
        high_vol= curr[5]['sorting_three']['volatility']['high']['range']
        high_fd_h= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['high']
        high_fd_m= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['medium']
        high_fd_l= curr[5]['sorting_three']['volatility']['high']['fractal_dimension']['low']
        medium_vol= curr[5]['sorting_three']['volatility']['medium']['range']
        medium_fd_h= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['high']
        medium_fd_m= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['medium']
        medium_fd_l= curr[5]['sorting_three']['volatility']['medium']['fractal_dimension']['low']
        low_vol= curr[5]['sorting_three']['volatility']['low']['range']
        low_fd_h= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['high']
        low_fd_m= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['medium']
        low_fd_l= curr[5]['sorting_three']['volatility']['low']['fractal_dimension']['low']
        for j, row in df.iterrows():
            if row['volatility'] >= high_vol[0] and row['volatility'] <= high_vol[1]:
                row['volatility']=1
                if row['fractal_dimension'] >= high_fd_h[0] and row['fractal_dimension'] <= high_fd_h[1]:
                    row['fractal_dimension']=1
                elif row['fractal_dimension'] >= high_fd_m[0] and row['fractal_dimension'] <= high_fd_m[1]:
                    row['fractal_dimension']=2
                elif row['fractal_dimension'] >= high_fd_l[0] and row['fractal_dimension'] <= high_fd_l[1]:
                    row['fractal_dimension']=3
            elif row['volatility'] >= medium_vol[0] and row['volatility'] <= medium_vol[1]:
                row['volatility']=2
                if row['fractal_dimension'] >= medium_fd_h[0] and row['fractal_dimension'] <= medium_fd_h[1]:
                    row['fractal_dimension']=1
                elif row['fractal_dimension'] >= medium_fd_m[0] and row['fractal_dimension'] <= medium_fd_m[1]:
                    row['fractal_dimension']=2
                elif row['fractal_dimension'] >= medium_fd_l[0] and row['fractal_dimension'] <= medium_fd_l[1]:
                    row['fractal_dimension']=3
            elif row['volatility'] >= low_vol[0] and row['volatility'] <= low_vol[1]:
                row['volatility']=3
                if row['fractal_dimension'] >= low_fd_h[0] and row['fractal_dimension'] <= low_fd_h[1]:
                    row['fractal_dimension']=1
                elif row['fractal_dimension'] >= low_fd_m[0] and row['fractal_dimension'] <= low_fd_m[1]:
                    row['fractal_dimension']=2
                elif row['fractal_dimension'] >= low_fd_l[0] and row['fractal_dimension'] <= low_fd_l[1]:
                    row['fractal_dimension']=3
        self.dfs[i].append(df)

    """
    This method will create three models for each currency pairs and train it.
    we will use check_metric() method to get the R2 values of the training predictions for and store it in self.predictions
    we will select the sorting method and model whose R2 value is minimum.
    we save the trained model and store the best sorting method for each currency pair in best_classification table.
    """

    def train_model(self):
        for index, df in enumerate(self.dfs):
            self.predictions.append([])
            print(df[0])
            print("___________________________")
            print(df[1])
            print("______________________________")
            print(df[2])
            print("_____________________________")
            reg1= pycr.setup(data= df[0].sample(frac=1).reset_index(drop=True), target= 'return') # initialize the model
            reg2= pycr.setup(data= df[1].sample(frac=1).reset_index(drop=True), target= 'return')
            reg3= pycr.setup(data= df[2].sample(frac=1).reset_index(drop=True), target= 'return')
            best1= pycr.compare_models(sort='RMSLE') # train the model on training set
            best2= pycr.compare_models(sort='RMSLE')
            best3= pycr.compare_models(sort='RMSLE')
            model1= pycr.create_model(best1) # we will create the model using best model we got from comapre_model
            model2= pycr.create_model(best2)
            model3= pycr.create_model(best3)
            t_model1= pycr.tune_model(model1) # we will fine tune default hyperparameters of the model
            t_model2= pycr.tune_model(model2)
            t_model3= pycr.tune_model(model3)
            final_model1= pycr.finalize_model(t_model1) # train the model on whole data set
            final_model2= pycr.finalize_model(t_model2)
            final_model3= pycr.finalize_model(t_model3)
            tpred1= pycr.predict_model(final_model1) # predict the return using the trained model
            tpred2= pycr.predict_model(final_model2)
            tpred3= pycr.predict_model(final_model3)
            l= list()
            l.append(pycu.check_metric(tpred1['return'], tpred1['Label'], 'R2'))  # we will get the mean R2 value of the predictions of this model
            l.append(pycu.check_metric(tpred2['return'], tpred2['Label'], 'R2'))
            l.append(pycu.check_metric(tpred3['return'], tpred3['Label'], 'R2'))
            print(l)
            self.predictions[index].extend(l) # storing predictions in self.predictions
            arr= np.array(self.predictions[index]) 
            ind= np.argmin(arr) # get index of minimum R2 value. That index indicate best sorting menthod
            # saving value in table. This will be use in real time predictions
            best_coll= self.db['best_classification']
            best_coll.insert_one({"_id":str(index),"best_sort":str(ind+1)})
            if ind==0:
                pycr.save_model(final_model1, self.currency_pairs[index][0]+self.currency_pairs[index][1]+"_model") # saving model.
            elif ind==1:
                pycr.save_model(final_model2, self.currency_pairs[index][0]+self.currency_pairs[index][1]+"_model")
            else:
                pycr.save_model(final_model3, self.currency_pairs[index][0]+self.currency_pairs[index][1]+"_model")

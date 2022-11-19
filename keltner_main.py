
from currency_conversion import KeltnerForexData

def main():
    # instantiate object of ForexData class and call forexdata() method
    # forexdata() method will call polygon api every other second for 24 hours.
    # it will create final.db sqlite file which will contains aggregated data of each currency pair , aggregated every 6 minutes.
    # As of now we have following currency pairs defined in forex_data.py library
    '''
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

    '''
    # you can append new currency pair as follows
    '''
    data= ForexxData()
    data.currency_pairs.append(["", ""])
    '''
    #data= ForexData()
    #data.forexdata()
    data= KeltnerForexData()
    data.keltner_forexdata() # collect vector of max, min, mean, vol, fd of different currency pairs in _agg tables for a period of 10 hours aggregated over a window of six minutes
    data.print_data() # print rows in _agg tables for all currency pairs.

if __name__=="__main__":
    main()

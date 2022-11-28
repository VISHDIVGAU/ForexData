
from currency_conversion import KeltnerForexData

def main():
    """
    # instantiate object of KeltnerForexData class and call keltner_forexdata() method
    # keltner_forexdata() method will call polygon api every second for 10 hours.
    # it will create final_test.db sqlite file which will contains _agg tables for each currency pairs
    # As of now we have following currency pairs defined in forex_data.py library
    
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

    """
    data= KeltnerForexData()
    data.keltner_forexdata() # collect vector of max, min, mean, vol, fd of different currency pairs in _agg tables for a period of 10 hours aggregated over a window of six minutes
    data.print_data() #  create csv files of  _agg tables in final_test.db

if __name__=="__main__":
    main()

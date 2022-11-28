
from currency_conversion import TrailingStopsData

def main():
    """
    # instantiate object of TrailingStopsData class and call trailing_forexdata()() method
    # trailing_forexdata() method will call polygon api every other second for 10 hours.
    # it will create final_trailingstops_updated.db sqlite file which will contains _agg tables each currency pair, and _sell tables for currency we are selling and _bought tables for currency we are buying
    # As of now we have following currency pairs defined in forex_data.py library
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

    
    # Currency we are selling are
    # self.short=["GBP","MXN","CZK","PLN","INR"]
    # Currency we are buying are
    # self.long= ["AUD","EUR","CAD","JPY","CNY"]
    """

    data= TrailingStopsData()
    data.trailing_forexdata() # collect vector of max, min, mean, vol, fd of different currency pairs in _agg tables for a period of 10 hours aggregated over a window of six minutes
    data.print_data() # print our total investment and profit and loss for each currency that we are selling and buying.
    data.create_csv() # create csv files for _agg tables, _sell and _bought tables in final_trailingstops_updated.db sqlite file

if __name__=="__main__":
    main()

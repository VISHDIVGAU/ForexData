
from currency_conversion import TrailingStopsTrainingData2

def main():
    """
    # instantiate object of TrailingStopsTrainingData2 class and call trailing_forex_trainingdata2() method
    # trailing_forexdata() method will call polygon api every other second for 10 hours.
    # it will collect data for 10 hours of each currency pair to train regression model to predict return
    """

    data= TrailingStopsTrainingData2()
    data.trailing_forex_trainingdata2() # collect vector of max, min, mean, vol, fd, return of different currency pairs in _agg tables for a period of 10 hours aggregated over a window of six minutes
    data.create_csv() # create csv files for _agg tables tables in final_trailingstops2_updated.db sqlite file

if __name__=="__main__":
    main()

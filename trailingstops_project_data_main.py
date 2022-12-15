
from currency_conversion import TrailingStopsProjectData

def main():
    """
    # instantiate object of TrailingStopsTrainingData3 class and call trailing_forex_trainingdata3() method
    # trailing_forex_trainingdata3() method will call polygon api every other second for 10 hours.
    # it will collect data for 10 hours of each currency pair to train regression model to predict return
    """

    data= TrailingStopsProjectData()
    data.trailing_forex_projectdata() # collect vector of max, min, mean, vol, fd, return of different currency pairs in _agg tables for a period of 10 hours aggregated over a window of six minutes
    data.create_csv() # create csv files for _agg tables collection in ForexData mongodb

if __name__=="__main__":
    main()

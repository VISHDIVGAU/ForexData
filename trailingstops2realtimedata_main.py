
from currency_conversion import TrailingStopsData2RealTimeData

def main():
    """
    # instantiate object of TrailingStopsData2RealTimeData class and call TrailingStopsData2RealTimeData()  method
    # In this we will classify in real time the fd and vol every six minutes using stored ranges of the sorting menthod stored in best classification table for that currency pair.
    # Wee will run this program for an hour. We will get next six minutes prediction every six minutes.
    we will be storng predicted return, actual return and error in model_performance tables.
    """

    data= TrailingStopsData2RealTimeData()
    data.trailing_forexdata2_real_time_data()
    data.create_csv() # create csv files for _model_perf tables in final_trailingstops_2_real.db sqlite file

if __name__=="__main__":
    main()

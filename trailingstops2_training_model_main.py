
from currency_conversion import TrailingStopsData2

def main():
    """
    # instantiate object of TrailingStopsData2 class and call all the three different sorting method to get the min and max ranges of all the three partitions of FD and volatility 
    # after sorting we classify the fd and volatility values as per the range we get from different sorting methods and store in self.dfs list.
    # then we train our model using all the three dataframes and choose the best sorting method to classify and model for that currency pair.
    we can see the R2 values predictions after training model on full data set using print(data.predictions)
    """

    data= TrailingStopsData2()
    data.reset_tables()
    data.sorting_one() # get the min and max range after sorting fd and volatility independently
    data.sorting_two() # get the min and max range after sorting fd first and then getting min and max range by sorting volatility coloumn of that partition.
    data.sorting_three() #get the min and max range after sorting volatility first and then getting min and max range by sorting fd coloumn of that partition.
    data.clasify() # classify values of fd and volatility in data frame after comparing with the ranges and storing all three data frames for each currency in self.dfs list.
    data.train_model() # it will train regression models using all the three data frames for each currency and select and save the model whoes R2 value is minimum. 
    print(data.predictions) # it will print the R2 values of the predictions we got in training.

if __name__=="__main__":
    main()

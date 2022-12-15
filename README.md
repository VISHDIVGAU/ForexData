# ForexData
Follow below steps before running main function
1) clone this repository in your local manchine
2) Pip install requiremnets.txt
3) create .env file on root directory
4) store polygon api key in .env file
    ```
    key=""
    ```
# Assignment 1
1) run below command in root directory on command line to fetch and store data 
    ```python:
    python3 main.py
    ```
    or
    Run two cell after cell 26 in the Jupiter notebook mmc639_Final_Exam-Copy1.

# Assignment 2
1) run below command in root directory on command line to fetch, store ,print and create csv files for currency pairs
   ```python:
   python3 keltner_main.py
   ```
# Assignment 3
1) run below command in root directory on command line to fetch, store, print and create csv files for currency pairs and currency we sell and currency we bought
  ```python:
	 python3 trailingstops_main.py
  ```
2) below image show the total investment and  profit or loss we made by buying and selling the currency. For currency we buy, profit will be positive, while for currency we sell, profit will be in negative.
![Result](https://github.com/VISHDIVGAU/ForexData/blob/main/profit_loss.png?raw=true)

# Assignment 4
This assignment is divided in three parts. We will get _model_perf.csv files for each currency pair.
1) We are collecting 10 hour data for all the given currency pairs. Run below command to collect data.
   ```python:
   python3 trailingstops2_training_data_main.py
   ```
2) We are training regression models for each currency pair. We have used three sorting method to classify fd, vol. We will storing best sorting menthod and best model for each currency pair. Run below command to train and get the best model for each currency pair.
  ```python:
  python3 trailingstops2_training_model_main.py
  ```
3) We are classifying FD and Vol in real time and getting predicted return for next six minutes using stored trained model for that currency pair. Run below command to get the predicted returns on real time data.
  ```python:
  python3 trailingstops2realtimedata_main.py
  ```
# Assignment 5
This assignment also divided in three parts. We will get _bought.csv and _sell.csv files with total investment made and profit or loss data.
I am using mongoDB to store the data. To install Mongodb locally in your computer please refer mongodbinstallation.md file.
1) We are collecting 10 hour data for all the given currency pairs. In this assignment Volatility is normalized. Run below command to collect data.
   ```python:
   python3 trailingstops3_training_data_main.py
   ```
2) We are training regression models for each currency pair. We have used three sorting method to classify fd, vol. We will storing best sorting menthod and best model for each currency pair. Run below command to train and get the best model for each currency pair.
  ```python:
  python3 trailingstops3_training_model_main.py
  ```
3) We are classifying FD and Vol in real time and getting predicted return for next six minutes and actual return and storing _agg collection. We are calculating profit or loss from actual return and predicted return. We will calculate percentage from the difference between actual return and predicted return. We will make the decision of investing more every hour according to below rules.

	a) If profit or loss from actual return and predicted return positive for the currency in buying list, then it means we are making profit and we invest further.
	
	b) If profit or loss from actual return and predicted return negative for the currency in buying list, then it means we are making loss and our investment decison depends if the percentage future loss is above the threshold value or not. If not, we will still invest further else we will not. The threshold value of percentage future loss decreases every hour from 0.25 on first hor, 0.15, second hour, 0.10, third hour, aand 0.05 from fourth hour onwards.
	
	c) If profit or loss from actual return and predicted return negative for the currency in selling list, then it means we are making profit and we invest further.
	
	d) If profit or loss from actual return and predicted return postive for the currency in selling list, then it means we are making loss and our investment decison depends if the percentage future loss is above the threshold value or not. If not, we will still invest further else we will not. The threshold value of percentage future loss decreases every hour from 0.25 on first hor, 0.15, second hour, 0.10, third hour, aand 0.05 from fourth hour onwards.  

At the end of ten hours data _bought and _sell collections we will have data regarding total initial investment, aggregate return, predicted aggregate return, avtual profit or loss, predicted profit or loss, and percentage future loss. 

Run below command to get the profit or loss and total investment on real time data.
```python:
  python3 trailingstops3realtimedata_main.py
 ```

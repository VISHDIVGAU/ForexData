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

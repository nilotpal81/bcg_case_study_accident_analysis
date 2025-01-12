### Analytics:
Application should perform below analysis and store the results for each analysis.

* Analysis 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
* Analysis 2: How many two-wheelers are booked for crashes?
* Analysis 3: Determine the Top 5 Vehicles made of the cars present in the crashes in which a driver died and Airbags did not deploy.
* Analysis 4: Determine the number of Vehicles with a driver having valid licences involved in hit-and-run?
* Analysis 5: Which state has the highest number of accidents in which females are not involved?
* Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death?
* Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.
* Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the contributing factor to a crash (Use Driver Zip Code)?
* Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.
* Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with     highest number of offenses (to be deduced from the data)

### Expected Output:
1. Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2. Code should be properly organized in folders as a project.
3. Input data sources and output should be config driven
4. Code should be strictly developed using Dataframe APIs (Do not use Spark SQL)
5. Share the entire project as zip or link to project in GitHub repo.

### Consideration:
1.Tested on windows

### Instructions to run application:
1. Clone the repo and follow these steps:

Steps:
1. Open terminal & go to the Project Directory: cd BCG_Big_Data_Case_Study
2. Create & activate a virtual env to run spark application in isolation
```bash
python -m venv venv
.\venv\Scripts\Activate
```
3. Install dependencies
```bash
pip install -r requirements.txt
```
4. Spark Submit
```bash
 spark-submit --master local[*] --conf spark.pyspark.python=python  file:///D:/BCG/main.py
```

**file:///D:/BCG/main.py** - Specifies the path to the main.py script on your machine. Make sure to replace this with the correct path if it's different on your system.

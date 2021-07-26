# Timesheets Pipeline

## ETL setup

1. Create an environment from `requirements.txt`
2. Input the file source to be extracted to folder `data`

## Database Setup
In file `base_operation.py` please configure your sql connection
```
host = <HOST>
port = <PORT>
user = <USER>>
password = <PASSWORD>
db = <DATABASE NAME>
```

## Run Pipeline Script
1. Open terminal and open folder `dag`
2. Run `python pipeline.py --csvfile <FILENAME> --datefilter <DATEFILTER>`
2. List of Arguments


| Name                  | Descriptions                                                                                                      |
| -------------------   | ----------------------------------------------------------------------------------------------------------------  |
| FILENAME              | Source file to be extracted. For example `timesheets.csv` or `employees.csv`                                      |
| DATEFILTER            | Filtered data to be extracted. Default detefilter is today - 1                                                    |


## Salary per Hour
There are two sql files that will be daily running:
- v_emp_work_duration.sql --> to gain information of employee's hour rate (can run monthly)
- summary.sql --> to gain information of salary per hour ( can run daily )


## Assumption Building

### Date Filter
1. If the source file came from `timesheets`, `datefilter` will be used based on `date`. 
2. If the source file came from `employee`, the `datefilter` will be based on `join_date`


### Duplicate Check
1. Duplicate data in `timesheets` checked by `date` and `employee_id`
2. Duplicate data in `employees` checked by `employee_id`

### Impropriate Checkin and Checkout
I found that there are condition where checkin time greater than checkout time.
After doing a simple research, I conclude that there are two conditions:
- users that have a mid night shift (these case occurs routinely)
- random users checkout the day after checkin. 
  Based on dicrete government, company can only give overtime not more than 14 hours a day.
  I assume that this company follows the rules and employee that have duration more than 14 hours as anomaly.
  The anomaly will be impute with the employees's hour rate.  
  To get more details, please check research.ipynb

### Null Values
Null values can't be drop since there might be a human error that forget to do check in and checkout.
So, I impute the null values based on:
1. Each of employee's rate hour if has record of work hour
2. All employees rate hour if has no record previously (must be occured one time -> for new employee) 

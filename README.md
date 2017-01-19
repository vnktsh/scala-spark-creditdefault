# Spark app to analyze credit card default data

## About project
Customers who own credit cards are expected to pay off their balances monthly. But, sometimes they failed to pay the bill and default.
Banks need to know the risk analysis of defaulted customers and predict likelihood of future payments or non-payments and 
act accordingly.

The dataset contains information about customers for past 6 month payment history
and their personal attributes.

Each row has following columns.

Column Name   | Description
------------- | -------------
CUSTID        | Customer ID
LIMIT_BAL     | Maximum Spending Limit for the customer
SEX           | F or M
EDUCATION     | Values : 1 (Graduate), 2 (University), 3 (High School) and 4 (Others)
MARRIAGE      | Values : 1 (Single), 2 ( Married) and 3 (Others)
AGE           | Age of the customer
PAY_1 to PAY_6          | Payment status for the last 6 months, one column for each month. Indicates the number of months (delay) the customer took to pay that month’s bill
BILL_AMT1 to BILL_AMT6  | The billed amount for credit card for each of the last 6 months.
PAY_AMT1 to PAY_AMT6    | The actual amount the customer paid for each of the last 6 months
DEFAULTED               | Whether the customer defaulted or not on the 7th month. The values are 0 (did not default) and 1 (defaulted)

##Insights
Is there a clear distinction between Males and females when it comes to the pattern of
defaulting? 

SEX_NAME  | Total | Defaults  | PER_DEFAULT
----------|-------|-----------|-------------
Female    | 591   | 218.0     | 37.0
Male      | 409   | 185.0     | 45.0

How does marital status and level of education affect the level of defaulting? Does one category
of customers default more than the other?

|MARR_DESC| ED_STR|Total|Defaults|PER_DEFAULT|
|---------|-----------|-----|--------|-----------|
| Married| Graduate| 268| 69.0| 26.0|
| Married|High School| 55| 24.0| 44.0|
| Married| Others| 4| 2.0| 50.0|
| Married| University| 243| 65.0| 27.0|
| Others| Graduate| 4| 4.0| 100.0|
| Others|High School| 8| 6.0| 75.0|
| Others| University| 7| 3.0| 43.0|
| Single| Graduate| 123| 71.0| 58.0|
| Single|High School| 87| 52.0| 60.0|
| Single| Others| 3| 2.0| 67.0|
| Single| University| 198| 105.0| 53.0|

Does the average payment delay for the previous 6 months provide any indication for the
customer to default in the future?

|AVG_PAY_DUR|Total|Defaults|PER_DEFAULT|
|-----------|-----|--------|-----------|
| 0.0| 356| 141.0| 40.0|
| 1.0| 552| 218.0| 39.0|
| 2.0| 85| 41.0| 48.0|
| 3.0| 4| 2.0| 50.0|
| 4.0| 3| 1.0| 33.0|

Does the average payment delay for the previous 6 months provide any indication for the
customer to default in the future?

|AVG_PAY_DUR|Total|Defaults|PER_DEFAULT|
------------|-----|--------|-----------|
| 0.0| 356| 141.0| 40.0|
| 1.0| 552| 218.0| 39.0|
| 2.0| 85| 41.0| 48.0|
| 3.0| 4| 2.0| 50.0|
| 4.0| 3| 1.0| 33.0|

##Prediction
Build and compare prediction models that can predict whether the customer is going to default in
the next month based on his/her history for the previous 6 months.

Machine learning classifier  | Accuracy 
----------|-------
Decision Tree    | 0.7417
Random Forest      | 0.7887
Naive Bayes | 0.7042

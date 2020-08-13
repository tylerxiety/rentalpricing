# Airbnb Forecasting and Pricing

Forecast the demand/orders of Airbnb listings and find the best pricing strategy to maximize rental revenue.

## 1.  Background and Problem Formulation

For an Airbnb host, one of the challenges is to set the 'right' prices (nightly rates) to get the highest possible rental revenues. 
The Airbnb platform has introduced several pricing tools, e.g. base price suggestion and dynamic pricing, but gives little explanation on those suggestions.
And based on my experience, those suggested prices given by the platform not necessarily yield good rental revenues. 
So I turn to data science for a second opinion.

One way to tackle a pricing problem is to break it down into two sub-problems: 
1) find the demand curve, i.e., how many nights would be booked at a given price; 
2) find the best prices to maximize the total revenue.

### 1.1. Find the Demand Curve
Finding the demand curve is a machine learning problem. And we can treat it either as a classification problem
or as a regression problem, depending on the actions a host takes against prices.
Host can set a base price and choose to turn on or off the dynamic pricing (link to Airbnb's dynamic pricing).
The question is how often the base price changes, e.g., a monthly base price, or weekly, daily, etc.
I put this into two scenarios: 

1. Monthly base prices. Then the question becomes predicting the monthly bookings at a given base prices, which is regression questions.

2. Dynamic base prices, i.e., the base prices can for a day or for a week. Then the question becomes a classification problem that predicts 
if a date would be booked at a given price.    

I call the first scenario a 'lazy' approach since host only needs to set prices monthly and it makes my life easier to aggregate the data into monthly sales.
Well, actually hosts are 'lazy' using both approaches since the models do all the work.


### 1.2. Find the best prices
Finding the best prices then is an optimization problem. And I consider two scenarios in the objectives:
1. No hosting cost: there is no cost if the property is booked,so the objective is to maximize the revenue. 
This usually means the host lives in the same property and take care of everything, so no direct extra cost incurred to the host if the property is booked.
2. There is hosting cost: a direct cost incurred for every booking needs to be considered in the objective. 
This usually means the host hires someone else to take care of the reservation.

## 2. Data and Analysis

The sample data covers almost all aspects of a listing, from property type, location, amenities to reviews, availabilities, prices, etc.
since 2017.
  
Findings:
1. 

## 3. Data Engineering
Features:

## 4. Modelling
Models:

## 5. Optimization
Optimization algorithms:

## 6. API


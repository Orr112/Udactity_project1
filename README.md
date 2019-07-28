# Data Modeling with Postgres
**Version 1.0.0**

## Table of contents
* [Intro](#Intro)
* [Motivation](#Motivation)
* [Files](#Files)
* [Technologies](#Technologies)
* [Libraries](#Libraries)
* [Install](#Install)
* [Analysis](#Analysis)
* [Acknowledgement](#Acknowledgement)

## Intro
The purpose of the Sparkify database is to gather user, artist, and song data into a relational database format. This star schema allows for data to be organized in an intitiuve manner, which will provide a source by which users can obtain account information or data for further analysis. 

## Motivation
The components of this project encompass important aspects of data warehousing. The design of this project begins with the implementation of a star schema using fact and dimension related tables. In order to populate the tables, data is extracted from two different sources - song and logs files. The data is then transformed so that the data is loadable into the created tables. As a whole, this project is a good example of how to design tables and perform ETL on different sources of data.   

Example fact and dimension tables:
![alt text](http://www.zentut.com/wp-content/uploads/2012/10/fact-table-example1.png)
Image provide by ZenTut website. 

## Files
- sql.py: this file contains create and insert table queries.
- create_tables.py: this file connects to the database, drops old tables and then creates ables from sql.py file.
- etl.py: this file extracts data from song and log files, transforms the data, and then loads the data into tables.
- test.ipynb: this notebook is used to perform basic data checks on data inserted into the tables.
- basicStats.ipynb: used to draw some basic user and song analysis. 


## Technologies
- Python 3.6
- jupyter
- SQL

## Libraries 
-psycopg2
-pandas
-glob
-os

## Install
Use [pip](https://pip.pypa.io/en/stable/)(package manager) to install psycopg2 and pandas.

```bash
pip intall psycopg2
pip install pandas
```
glob and os come pre-installed as they are part of python standard library.


## Analysis 

### Demogoraphics
```
--Total Users
select count(*) from users

--Percentage Users in Paid Level
Select (count(*)*100) / 84  AS PercentPaidLevel from users where level = 'paid'

--Percent Paid User by Identified Gender
Select (count(*)*100) / 84  AS PercentFemalePaidLevel from users where gender = 'F' and level = 'paid'

Select (count(*)*100) / 84  AS PercentMale from users where gender = 'M' and level = 'paid'

--User Gender Breakdown
Select (count(*)*100) / 84  AS PercentMale from users where gender = 'M'

Select (count(*)*100) / 84  AS PercentFemale from users where gender = 'F'

```
### Artist/Song Analysis
```
--Max Song Length
select (max(duration))/60 AS MaxSongLength from songs 

--Percentage Song Longer than Average
Select (count(*)*100)/72 AS PercentSongsGtAverage from artists a Join songs s on a.artist_id = s.artist_id  where duration >= 219

```

## Acknowledgement
- Facts and Dimensions Table layout above provide by ZenTut.
- This project was peformed as part of the Udacity Data Engineering Nanodegree Program.
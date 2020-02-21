import json
import os

import numpy as np
from sorted_months_weekdays import *
from sort_dataframeby_monthorweek import *
import pandas as pd
# import seaborn as sns
# import matplotlib.pyplot as plt
# import tkinter
# import matplotlib
# matplotlib.use('TkAgg')
from django.http import JsonResponse


df = pd.read_csv("Transaction_Sample_DataSet-2019.csv")
pd.options.display.float_format = 'GHS {:,.2f}'.format
# print(df.head())


def index(request):
    return JsonResponse('hhhh', safe=False)


def total_amount(request):

    return JsonResponse((df['amount'].sum()), safe=False)


def service_count(request):
    service_count = df.groupby(['name'])['amount'].count().reset_index()
    # service_count.set_index('name', inplace=True)
    service_count = service_count.to_dict('r')

    json_data = JsonResponse(service_count, safe=False)
    return json_data


def service_revenue(request):
    service_revenue = df.groupby(['name'])['amount'].sum().reset_index()
    service_revenue = service_revenue.to_dict('r')
    json_data = JsonResponse(service_revenue, safe=False)
    return json_data


def status_count(request):
    status_count = df.groupby(['status'])['amount'].count().reset_index()
    status_count = status_count.to_dict('r')
    json_data = JsonResponse(status_count, safe=False)
    return json_data

df['datecreated'] = pd.to_datetime(df['datecreated'])
df['month'] = df['datecreated'].dt.month_name()
df['weekday'] = df['datecreated'].dt.weekday_name
df['day'] = df['datecreated'].dt.day
df['hour'] = df['datecreated'].dt.hour


def monthly_average_trnx(request):
    monthly_avg_revenue = df.groupby(['month'])['amount'].mean().reset_index()
    monthly_avg_revenue = Sort_Dataframeby_Month(df=monthly_avg_revenue, monthcolumnname='month')
    monthly_avg_revenue = monthly_avg_revenue.to_dict('r')
    json_data = JsonResponse(monthly_avg_revenue, safe=False)
    return json_data


def monthly_revenue(request):
    monthly_revenue = df.groupby(['month'])['amount'].sum().reset_index()
    monthly_revenue = Sort_Dataframeby_Month(df=monthly_revenue, monthcolumnname='month')
    monthly_revenue = monthly_revenue.to_dict('r')
    json_data = JsonResponse(monthly_revenue, safe=False)
    return json_data


def monthly_count(request):
    monthly_count = df.groupby(['month'])['amount'].count().reset_index()
    monthly_count = Sort_Dataframeby_Month(df=monthly_count, monthcolumnname='month')
    monthly_count = monthly_count.to_dict('r')
    json_data = JsonResponse(monthly_count, safe=False)
    return json_data


def weekday_revenue(request):
    weekday_revenue = df.groupby(['weekday'])['amount'].sum().reset_index()
    weekday_revenue = Sort_Dataframeby_Weekday(df=weekday_revenue, Weekdaycolumnname='weekday')
    weekday_revenue = weekday_revenue.to_dict('r')
    json_data = JsonResponse(weekday_revenue, safe=False)
    return json_data

def weekday_count(request):
    weekday_count = df.groupby(['weekday'])['amount'].count().reset_index()
    weekday_count = Sort_Dataframeby_Weekday(df=weekday_count, Weekdaycolumnname='weekday')
    weekday_count = weekday_count.to_dict('r')
    json_data = JsonResponse(weekday_count, safe=False)
    return json_data


def daily_revenue(request):
    daily_revenue = df.groupby(['day'])['amount'].sum().reset_index()
    daily_revenue = daily_revenue.to_dict('r')
    json_data = JsonResponse(daily_revenue, safe=False)
    return json_data


def daily_count(request):
    daily_count = df.groupby(['day'])['amount'].count().reset_index()
    daily_count = daily_count.to_dict('r')
    json_data = JsonResponse(daily_count, safe=False)
    return json_data


def hourly_revenue(request):
    hourly_revenue = df.groupby(['hour'])['amount'].sum().reset_index()
    hourly_revenue = hourly_revenue.to_dict('r')
    json_data = JsonResponse(hourly_revenue, safe=False)
    return json_data


def hourly_count(request):
    hourly_count = df.groupby(['hour'])['amount'].count().reset_index()
    hourly_count = hourly_count.to_dict('r')
    json_data = JsonResponse(hourly_count, safe=False)
    return json_data


def monthly_airtime_count(request):
    monthly_airtime_count = pd.pivot_table(data=df[df['name'] == 'Airtime'], index='month', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'monthly_airtime_count'})
    monthly_airtime_count = Sort_Dataframeby_Month(df=monthly_airtime_count, monthcolumnname='month')
    monthly_airtime_count = monthly_airtime_count.to_dict('r')
    json_data = JsonResponse(monthly_airtime_count, safe=False)
    return json_data


def monthly_airtime_revenue(request):
    monthly_airtime_revenue = pd.pivot_table(data=df[df['name'] == 'Airtime'], index='month', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'monthly_airtime_revenue'})
    monthly_airtime_revenue = Sort_Dataframeby_Month(df=monthly_airtime_revenue, monthcolumnname='month')
    monthly_airtime_revenue = monthly_airtime_revenue.to_dict('r')
    json_data = JsonResponse(monthly_airtime_revenue, safe=False)
    return json_data


def weekday_airtime_count(request):
    weekday_airtime_count = pd.pivot_table(data=df[df['name'] == 'Airtime'], index='weekday', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'weekday_airtime_count'})
    weekday_airtime_count = Sort_Dataframeby_Weekday(df=weekday_airtime_count,Weekdaycolumnname='weekday')
    weekday_airtime_count = weekday_airtime_count.to_dict('r')
    json_data = JsonResponse(weekday_airtime_count, safe=False)
    return json_data


def weekday_airtime_revenue(request):
    weekday_airtime_revenue = pd.pivot_table(data=df[df['name'] == 'Airtime'], index='weekday', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'weekday_airtime_revenue'})
    weekday_airtime_revenue = Sort_Dataframeby_Weekday(df=weekday_airtime_revenue,Weekdaycolumnname='weekday')
    weekday_airtime_revenue = weekday_airtime_revenue.to_dict('r')
    json_data = JsonResponse(weekday_airtime_revenue, safe=False)
    return json_data


def daily_airtime_count(request):
    daily_airtime_count = pd.pivot_table(data=df[df['name'] == 'Airtime'], index='day', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'daily_airtime_count'})
    daily_airtime_count = daily_airtime_count.to_dict('r')
    json_data = JsonResponse(daily_airtime_count, safe=False)
    return json_data


def daily_airtime_revenue(request):
    daily_airtime_revenue = pd.pivot_table(data=df[df['name'] == 'Airtime'], index='day', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'daily_airtime_revenue'})
    daily_airtime_revenue = daily_airtime_revenue.to_dict('r')
    json_data = JsonResponse(daily_airtime_revenue, safe=False)
    return json_data


def hourly_airtime_count(request):
    hourly_airtime_count = pd.pivot_table(data=df[df['name'] == 'Airtime'], index='hour', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'hourly_airtime_count'})
    hourly_airtime_count = hourly_airtime_count.to_dict('r')
    json_data = JsonResponse(hourly_airtime_count, safe=False)
    return json_data


def hourly_airtime_revenue(request):
    hourly_airtime_revenue = pd.pivot_table(data=df[df['name'] == 'Airtime'], index='hour', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'hourly_airtime_revenue'})
    hourly_airtime_revenue = hourly_airtime_revenue.to_dict('r')
    json_data = JsonResponse(hourly_airtime_revenue, safe=False)
    return json_data


def jan_weekday_airtime_count(request):
    jan_weekday_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_weekday_airtime_count'})
    jan_weekday_airtime_count = Sort_Dataframeby_Weekday(df=jan_weekday_airtime_count, Weekdaycolumnname='weekday')
    jan_weekday_airtime_count = jan_weekday_airtime_count.to_dict('r')
    json_data = JsonResponse(jan_weekday_airtime_count, safe=False)
    return json_data

def jan_weekday_airtime_revenue(request):
    jan_weekday_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_weekday_airtime_revenue'})
    jan_weekday_airtime_revenue = Sort_Dataframeby_Weekday(df=jan_weekday_airtime_revenue, Weekdaycolumnname='weekday')
    jan_weekday_airtime_revenue = jan_weekday_airtime_revenue.to_dict('r')
    json_data = JsonResponse(jan_weekday_airtime_revenue, safe=False)
    return json_data

def jan_daily_airtime_count(request):
    jan_daily_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_daily_airtime_count'})
    jan_daily_airtime_count = jan_daily_airtime_count.to_dict('r')
    json_data = JsonResponse(jan_daily_airtime_count, safe=False)
    return json_data

def jan_daily_airtime_revenue(request):
    jan_daily_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_daily_airtime_revenue'})
    jan_daily_airtime_revenue = jan_daily_airtime_revenue.to_dict('r')
    json_data = JsonResponse(jan_daily_airtime_revenue, safe=False)
    return json_data

def jan_hourly_airtime_count(request):
    jan_hourly_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_hourly_airtime_count'})
    jan_hourly_airtime_count = jan_hourly_airtime_count.to_dict('r')
    json_data = JsonResponse(jan_hourly_airtime_count, safe=False)
    return json_data


def jan_hourly_airtime_revenue(request):
    jan_hourly_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_hourly_airtime_sum'})
    jan_hourly_airtime_revenue = jan_hourly_airtime_revenue.to_dict('r')
    json_data = JsonResponse(jan_hourly_airtime_revenue, safe=False)
    return json_data


def feb_weekday_airtime_count(request):
    feb_weekday_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_weekday_airtime_count'})
    feb_weekday_airtime_count = Sort_Dataframeby_Weekday(df=feb_weekday_airtime_count, Weekdaycolumnname='weekday')
    feb_weekday_airtime_count = feb_weekday_airtime_count.to_dict('r')
    json_data = JsonResponse(feb_weekday_airtime_count, safe=False)
    return json_data


def feb_weekday_airtime_revenue(request):
    feb_weekday_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_weekday_airtime_revenue'})
    feb_weekday_airtime_revenue = Sort_Dataframeby_Weekday(df=feb_weekday_airtime_revenue, Weekdaycolumnname='weekday')
    feb_weekday_airtime_revenue = feb_weekday_airtime_revenue.to_dict('r')
    json_data = JsonResponse(feb_weekday_airtime_revenue, safe=False)
    return json_data


def feb_daily_airtime_count(request):
    feb_daily_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_daily_airtime_count'})
    feb_daily_airtime_count = feb_daily_airtime_count.to_dict('r')
    json_data = JsonResponse(feb_daily_airtime_count, safe=False)
    return json_data


def feb_daily_airtime_revenue(request):
    feb_daily_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_daily_airtime_revenue'})
    feb_daily_airtime_revenue = feb_daily_airtime_revenue.to_dict('r')
    json_data = JsonResponse(feb_daily_airtime_revenue, safe=False)
    return json_data


def feb_hourly_airtime_count(request):
    feb_hourly_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_hourly_airtime_count'})
    feb_hourly_airtime_count = feb_hourly_airtime_count.to_dict('r')
    json_data = JsonResponse(feb_hourly_airtime_count, safe=False)
    return json_data

def feb_hourly_airtime_revenue(request):
    feb_hourly_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_hourly_airtime_revenue'})
    feb_hourly_airtime_revenue = feb_hourly_airtime_revenue.to_dict('r')
    json_data = JsonResponse(feb_hourly_airtime_revenue, safe=False)
    return json_data


def mar_weekday_airtime_count(request):
    mar_weekday_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_weekday_airtime_count'})
    mar_weekday_airtime_count = Sort_Dataframeby_Weekday(df=mar_weekday_airtime_count, Weekdaycolumnname='weekday')
    mar_weekday_airtime_count = mar_weekday_airtime_count.to_dict('r')
    json_data = JsonResponse(mar_weekday_airtime_count, safe=False)
    return json_data


def mar_weekday_airtime_revenue(request):
    mar_weekday_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_weekday_airtime_revenue'})
    mar_weekday_airtime_revenue = Sort_Dataframeby_Weekday(df=mar_weekday_airtime_revenue, Weekdaycolumnname='weekday')
    mar_weekday_airtime_revenue = mar_weekday_airtime_revenue.to_dict('r')
    json_data = JsonResponse(mar_weekday_airtime_revenue, safe=False)
    return json_data


def mar_daily_airtime_count(request):
    mar_daily_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_daily_airtime_count'})
    mar_daily_airtime_count = mar_daily_airtime_count.to_dict('r')
    json_data = JsonResponse(mar_daily_airtime_count, safe=False)
    return json_data


def mar_daily_airtime_revenue(request):
    mar_daily_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_daily_airtime_revenue'})
    mar_daily_airtime_revenue = mar_daily_airtime_revenue.to_dict('r')
    json_data = JsonResponse(mar_daily_airtime_revenue, safe=False)
    return json_data


def mar_hourly_airtime_count(request):
    mar_hourly_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_hourly_airtime_count'})
    mar_hourly_airtime_count = mar_hourly_airtime_count.to_dict('r')
    json_data = JsonResponse(mar_hourly_airtime_count, safe=False)
    return json_data


def mar_hourly_airtime_revenue(request):
    mar_hourly_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_hourly_airtime_revenue'})
    mar_hourly_airtime_revenue = mar_hourly_airtime_revenue.to_dict('r')
    json_data = JsonResponse(mar_hourly_airtime_revenue, safe=False)
    return json_data


def apr_weekday_airtime_count(request):
    apr_weekday_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_weekday_airtime_count'})
    apr_weekday_airtime_count = Sort_Dataframeby_Weekday(df=apr_weekday_airtime_count, Weekdaycolumnname='weekday')
    apr_weekday_airtime_count = apr_weekday_airtime_count.to_dict('r')
    json_data = JsonResponse(apr_weekday_airtime_count, safe=False)
    return json_data

def apr_weekday_airtime_revenue(request):
    apr_weekday_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_weekday_airtime_revenue'})
    apr_weekday_airtime_revenue = Sort_Dataframeby_Weekday(df=apr_weekday_airtime_revenue, Weekdaycolumnname='weekday')
    apr_weekday_airtime_revenue = apr_weekday_airtime_revenue.to_dict('r')
    json_data = JsonResponse(apr_weekday_airtime_revenue, safe=False)
    return json_data


def apr_daily_airtime_count(request):
    apr_daily_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_daily_airtime_count'})
    apr_daily_airtime_count = apr_daily_airtime_count.to_dict('r')
    json_data = JsonResponse(apr_daily_airtime_count, safe=False)
    return json_data


def apr_daily_airtime_revenue(request):
    apr_daily_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_daily_airtime_revenue'})
    apr_daily_airtime_revenue = apr_daily_airtime_revenue.to_dict('r')
    json_data = JsonResponse(apr_daily_airtime_revenue, safe=False)
    return json_data


def apr_hourly_airtime_count(request):
    apr_hourly_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_hourly_airtime_count'})
    apr_hourly_airtime_count = apr_hourly_airtime_count.to_dict('r')
    json_data = JsonResponse(apr_hourly_airtime_count, safe=False)
    return json_data


def apr_hourly_airtime_revenue(request):
    apr_hourly_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_hourly_airtime_revenue'})
    apr_hourly_airtime_revenue = apr_hourly_airtime_revenue.to_dict('r')
    json_data = JsonResponse(apr_hourly_airtime_revenue, safe=False)
    return json_data


def may_weekday_airtime_count(request):
    may_weekday_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_weekday_airtime_count'})
    may_weekday_airtime_count = Sort_Dataframeby_Weekday(df=may_weekday_airtime_count, Weekdaycolumnname='weekday')
    may_weekday_airtime_count = may_weekday_airtime_count.to_dict('r')
    json_data = JsonResponse(may_weekday_airtime_count, safe=False)
    return json_data

def may_weekday_airtime_revenue(request):
    may_weekday_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_weekday_airtime_revenue'})
    may_weekday_airtime_revenue = Sort_Dataframeby_Weekday(df=may_weekday_airtime_revenue, Weekdaycolumnname='weekday')
    may_weekday_airtime_revenue = may_weekday_airtime_revenue.to_dict('r')
    json_data = JsonResponse(may_weekday_airtime_revenue, safe=False)
    return json_data


def may_daily_airtime_count(request):
    may_daily_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_daily_airtime_count'})
    may_daily_airtime_count = may_daily_airtime_count.to_dict('r')
    json_data = JsonResponse(may_daily_airtime_count, safe=False)
    return json_data


def may_daily_airtime_revenue(request):
    may_daily_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_daily_airtime_revenue'})
    may_daily_airtime_revenue = may_daily_airtime_revenue.to_dict('r')
    json_data = JsonResponse(may_daily_airtime_revenue, safe=False)
    return json_data

def may_hourly_airtime_count(request):
    may_hourly_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_hourly_airtime_count'})
    may_hourly_airtime_count = may_hourly_airtime_count.to_dict('r')
    json_data = JsonResponse(may_hourly_airtime_count, safe=False)
    return json_data


def may_hourly_airtime_revenue(request):
    may_hourly_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_hourly_airtime_revenue'})
    may_hourly_airtime_revenue = may_hourly_airtime_revenue.to_dict('r')
    json_data = JsonResponse(may_hourly_airtime_revenue, safe=False)
    return json_data


def jun_weekday_airtime_count(request):
    jun_weekday_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_weekday_airtime_count'})
    jun_weekday_airtime_count = Sort_Dataframeby_Weekday(df=jun_weekday_airtime_count, Weekdaycolumnname='weekday')
    jun_weekday_airtime_count = jun_weekday_airtime_count.to_dict('r')
    json_data = JsonResponse(jun_weekday_airtime_count, safe=False)
    return json_data


def jun_weekday_airtime_revenue(request):
    jun_weekday_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_weekday_airtime_revenue'})
    jun_weekday_airtime_revenue = Sort_Dataframeby_Weekday(df=jun_weekday_airtime_revenue, Weekdaycolumnname='weekday')
    jun_weekday_airtime_revenue = jun_weekday_airtime_revenue.to_dict('r')
    json_data = JsonResponse(jun_weekday_airtime_revenue, safe=False)
    return json_data


def jun_daily_airtime_count(request):
    jun_daily_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_daily_airtime_count'})
    jun_daily_airtime_count = jun_daily_airtime_count.to_dict('r')
    json_data = JsonResponse(jun_daily_airtime_count, safe=False)
    return json_data


def jun_daily_airtime_revenue(request):
    jun_daily_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_daily_airtime_revenue'})
    jun_daily_airtime_revenue = jun_daily_airtime_revenue.to_dict('r')
    json_data = JsonResponse(jun_daily_airtime_revenue, safe=False)
    return json_data


def jun_hourly_airtime_count(request):
    jun_hourly_airtime_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_hourly_airtime_count'})
    jun_hourly_airtime_count = jun_hourly_airtime_count.to_dict('r')
    json_data = JsonResponse(jun_hourly_airtime_count, safe=False)
    return json_data


def jun_hourly_airtime_revenue(request):
    jun_hourly_airtime_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Airtime']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_hourly_airtime_revenue'})
    jun_hourly_airtime_revenue = jun_hourly_airtime_revenue.to_dict('r')
    json_data = JsonResponse(jun_hourly_airtime_revenue, safe=False)
    return json_data


def monthly_momo_count(request):
    monthly_momo_count = pd.pivot_table(data=df[df['name'] == 'Momo Interoperability'], index='month', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'monthly_momo_count'})
    monthly_momo_count = Sort_Dataframeby_Month(df=monthly_momo_count, monthcolumnname='month')
    monthly_momo_count = monthly_momo_count.to_dict('r')
    json_data = JsonResponse(monthly_momo_count, safe=False)
    return json_data


def monthly_momo_revenue(request):
    monthly_momo_revenue = pd.pivot_table(data=df[df['name'] == 'Momo Interoperability'], index='month', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'monthly_momo_revenue'})
    monthly_momo_revenue = Sort_Dataframeby_Month(df=monthly_momo_revenue, monthcolumnname='month')
    monthly_momo_revenue = monthly_momo_revenue.to_dict('r')
    json_data = JsonResponse(monthly_momo_revenue, safe=False)
    return json_data


def weekday_momo_count(request):
    weekday_momo_count = pd.pivot_table(data=df[df['name'] == 'Momo Interoperability'], index='weekday', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'weekday_momo_count'})
    weekday_momo_count = Sort_Dataframeby_Weekday(df=weekday_momo_count,Weekdaycolumnname='weekday')
    weekday_momo_count = weekday_momo_count.to_dict('r')
    json_data = JsonResponse(weekday_momo_count, safe=False)
    return json_data


def weekday_momo_revenue(request):
    weekday_momo_revenue = pd.pivot_table(data=df[df['name'] == 'Momo Interoperability'], index='weekday', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'weekday_momo_revenue'})
    weekday_momo_revenue = Sort_Dataframeby_Weekday(df=weekday_momo_revenue,Weekdaycolumnname='weekday')
    weekday_momo_revenue = weekday_momo_revenue.to_dict('r')
    json_data = JsonResponse(weekday_momo_revenue, safe=False)
    return json_data


def daily_momo_count(request):
    daily_momo_count = pd.pivot_table(data=df[df['name'] == 'Momo Interoperability'], index='day', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'daily_momo_count'})
    daily_momo_count = daily_momo_count.to_dict('r')
    json_data = JsonResponse(daily_momo_count, safe=False)
    return json_data


def daily_momo_revenue(request):
    daily_momo_revenue = pd.pivot_table(data=df[df['name'] == 'Momo Interoperability'], index='day', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'daily_momo_revenue'})
    daily_momo_revenue = daily_momo_revenue.to_dict('r')
    json_data = JsonResponse(daily_momo_revenue, safe=False)
    return json_data


def hourly_momo_count(request):
    hourly_momo_count = pd.pivot_table(data=df[df['name'] == 'Momo Interoperability'], index='hour', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'hourly_momo_count'})
    hourly_momo_count = hourly_momo_count.to_dict('r')
    json_data = JsonResponse(hourly_momo_count, safe=False)
    return json_data


def hourly_momo_revenue(request):
    hourly_momo_revenue = pd.pivot_table(data=df[df['name'] == 'Momo Interoperability'], index='hour', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'hourly_momo_revenue'})
    hourly_momo_revenue = hourly_momo_revenue.to_dict('r')
    json_data = JsonResponse(hourly_momo_revenue, safe=False)
    return json_data


def jan_weekday_momo_count(request):
    jan_weekday_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_weekday_momo_count'})
    jan_weekday_momo_count = Sort_Dataframeby_Weekday(df=jan_weekday_momo_count,Weekdaycolumnname='weekday')
    jan_weekday_momo_count = jan_weekday_momo_count.to_dict('r')
    json_data = JsonResponse(jan_weekday_momo_count, safe=False)
    return json_data

def jan_weekday_momo_revenue(request):
    jan_weekday_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_weekday_momo_revenue'})
    jan_weekday_momo_revenue = Sort_Dataframeby_Weekday(df=jan_weekday_momo_revenue,Weekdaycolumnname='weekday')
    jan_weekday_momo_revenue = jan_weekday_momo_revenue.to_dict('r')
    json_data = JsonResponse(jan_weekday_momo_revenue, safe=False)
    return json_data


def jan_daily_momo_count(request):
    jan_daily_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_daily_momo_count'})
    jan_daily_momo_count = jan_daily_momo_count.to_dict('r')
    json_data = JsonResponse(jan_daily_momo_count, safe=False)
    return json_data


def jan_daily_momo_revenue(request):
    jan_daily_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_daily_momo_revenue'})
    jan_daily_momo_revenue = jan_daily_momo_revenue.to_dict('r')
    json_data = JsonResponse(jan_daily_momo_revenue, safe=False)
    return json_data


def jan_hourly_momo_count(request):
    jan_hourly_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_hourly_momo_count'})
    jan_hourly_momo_count = jan_hourly_momo_count.to_dict('r')
    json_data = JsonResponse(jan_hourly_momo_count, safe=False)
    return json_data


def jan_hourly_momo_revenue(request):
    jan_hourly_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['January']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_hourly_momo_revenue'})
    jan_hourly_momo_revenue = jan_hourly_momo_revenue.to_dict('r')
    json_data = JsonResponse(jan_hourly_momo_revenue, safe=False)
    return json_data


def feb_weekday_momo_count(request):
    feb_weekday_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_weekday_momo_count'})
    feb_weekday_momo_count = Sort_Dataframeby_Weekday(df=feb_weekday_momo_count,Weekdaycolumnname='weekday')
    feb_weekday_momo_count = feb_weekday_momo_count.to_dict('r')
    json_data = JsonResponse(feb_weekday_momo_count, safe=False)
    return json_data


def feb_weekday_momo_revenue(request):
    feb_weekday_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_weekday_momo_sum'})
    feb_weekday_momo_revenue = Sort_Dataframeby_Weekday(df=feb_weekday_momo_revenue,Weekdaycolumnname='weekday')
    feb_weekday_momo_revenue = feb_weekday_momo_revenue.to_dict('r')
    json_data = JsonResponse(feb_weekday_momo_revenue, safe=False)
    return json_data


def feb_daily_momo_count(request):
    feb_daily_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_daily_momo_count'})
    feb_daily_momo_count = feb_daily_momo_count.to_dict('r')
    json_data = JsonResponse(feb_daily_momo_count, safe=False)
    return json_data


def feb_daily_momo_revenue(request):
    feb_daily_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_daily_momo_revenue'})
    feb_daily_momo_revenue = feb_daily_momo_revenue.to_dict('r')
    json_data = JsonResponse(feb_daily_momo_revenue, safe=False)
    return json_data


def feb_hourly_momo_count(request):
    feb_hourly_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_hourly_momo_count'})
    feb_hourly_momo_count = feb_hourly_momo_count.to_dict('r')
    json_data = JsonResponse(feb_hourly_momo_count, safe=False)
    return json_data


def feb_hourly_momo_revenue(request):
    feb_hourly_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['February']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_hourly_momo_revenue'})
    feb_hourly_momo_revenue = feb_hourly_momo_revenue.to_dict('r')
    json_data = JsonResponse(feb_hourly_momo_revenue, safe=False)
    return json_data


def mar_weekday_momo_count(request):
    mar_weekday_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_weekday_momo_count'})
    mar_weekday_momo_count = Sort_Dataframeby_Weekday(df=mar_weekday_momo_count,Weekdaycolumnname='weekday')
    mar_weekday_momo_count = mar_weekday_momo_count.to_dict('r')
    json_data = JsonResponse(mar_weekday_momo_count, safe=False)
    return json_data


def mar_weekday_momo_revenue(request):
    mar_weekday_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_weekday_momo_revenue'})
    mar_weekday_momo_revenue = Sort_Dataframeby_Weekday(df=mar_weekday_momo_revenue,Weekdaycolumnname='weekday')
    mar_weekday_momo_revenue = mar_weekday_momo_revenue.to_dict('r')
    json_data = JsonResponse(mar_weekday_momo_revenue, safe=False)
    return json_data


def mar_daily_momo_count(request):
    mar_daily_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_daily_momo_count'})
    mar_daily_momo_count = mar_daily_momo_count.to_dict('r')
    json_data = JsonResponse(mar_daily_momo_count, safe=False)
    return json_data


def mar_daily_momo_revenue(request):
    mar_daily_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_daily_momo_revenue'})
    mar_daily_momo_revenue = mar_daily_momo_revenue.to_dict('r')
    json_data = JsonResponse(mar_daily_momo_revenue, safe=False)
    return json_data


def mar_hourly_momo_count(request):
    mar_hourly_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_hourly_momo_count'})
    mar_hourly_momo_count = mar_hourly_momo_count.to_dict('r')
    json_data = JsonResponse(mar_hourly_momo_count, safe=False)
    return json_data


def mar_hourly_momo_revenue(request):
    mar_hourly_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['March']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_hourly_momo_revenue'})
    mar_hourly_momo_revenue = mar_hourly_momo_revenue.to_dict('r')
    json_data = JsonResponse(mar_hourly_momo_revenue, safe=False)
    return json_data


def apr_weekday_momo_count(request):
    apr_weekday_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_weekday_momo_count'})
    apr_weekday_momo_count = Sort_Dataframeby_Weekday(df=apr_weekday_momo_count,Weekdaycolumnname='weekday')
    apr_weekday_momo_count = apr_weekday_momo_count.to_dict('r')
    json_data = JsonResponse(apr_weekday_momo_count, safe=False)
    return json_data


def apr_weekday_momo_revenue(request):
    apr_weekday_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_weekday_momo_revenue'})
    apr_weekday_momo_revenue = Sort_Dataframeby_Weekday(df=apr_weekday_momo_revenue,Weekdaycolumnname='weekday')
    apr_weekday_momo_revenue = apr_weekday_momo_revenue.to_dict('r')
    json_data = JsonResponse(apr_weekday_momo_revenue, safe=False)
    return json_data


def apr_daily_momo_count(request):
    apr_daily_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_daily_momo_count'})
    apr_daily_momo_count = apr_daily_momo_count.to_dict('r')
    json_data = JsonResponse(apr_daily_momo_count, safe=False)
    return json_data


def apr_daily_momo_revenue(request):
    apr_daily_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_daily_momo_revenue'})
    apr_daily_momo_revenue = apr_daily_momo_revenue.to_dict('r')
    json_data = JsonResponse(apr_daily_momo_revenue, safe=False)
    return json_data


def apr_hourly_momo_count(request):
    apr_hourly_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_hourly_momo_count'})
    apr_hourly_momo_count = apr_hourly_momo_count.to_dict('r')
    json_data = JsonResponse(apr_hourly_momo_count, safe=False)
    return json_data


def apr_hourly_momo_revenue(request):
    apr_hourly_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['April']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_hourly_momo_revenue'})
    apr_hourly_momo_revenue = apr_hourly_momo_revenue.to_dict('r')
    json_data = JsonResponse(apr_hourly_momo_revenue, safe=False)
    return json_data


def may_weekday_momo_count(request):
    may_weekday_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_weekday_momo_count'})
    may_weekday_momo_count = Sort_Dataframeby_Weekday(df=may_weekday_momo_count,Weekdaycolumnname='weekday')
    may_weekday_momo_count = may_weekday_momo_count.to_dict('r')
    json_data = JsonResponse(may_weekday_momo_count, safe=False)
    return json_data


def may_weekday_momo_revenue(request):
    may_weekday_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_weekday_momo_revenue'})
    may_weekday_momo_revenue = Sort_Dataframeby_Weekday(df=may_weekday_momo_revenue,Weekdaycolumnname='weekday')
    may_weekday_momo_revenue = may_weekday_momo_revenue.to_dict('r')
    json_data = JsonResponse(may_weekday_momo_revenue, safe=False)
    return json_data


def may_daily_momo_count(request):
    may_daily_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_daily_momo_count'})
    may_daily_momo_count = may_daily_momo_count.to_dict('r')
    json_data = JsonResponse(may_daily_momo_count, safe=False)
    return json_data


def may_daily_momo_revenue(request):
    may_daily_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_daily_momo_revenue'})
    may_daily_momo_revenue = may_daily_momo_revenue.to_dict('r')
    json_data = JsonResponse(may_daily_momo_revenue, safe=False)
    return json_data


def may_hourly_momo_count(request):
    may_hourly_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_hourly_momo_count'})
    may_hourly_momo_count = may_hourly_momo_count.to_dict('r')
    json_data = JsonResponse(may_hourly_momo_count, safe=False)
    return json_data


def may_hourly_momo_revenue(request):
    may_hourly_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['May']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_hourly_momo_revenue'})
    may_hourly_momo_revenue = may_hourly_momo_revenue.to_dict('r')
    json_data = JsonResponse(may_hourly_momo_revenue, safe=False)
    return json_data


def jun_weekday_momo_count(request):
    jun_weekday_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_weekday_momo_count'})
    jun_weekday_momo_count = Sort_Dataframeby_Weekday(df=jun_weekday_momo_count,Weekdaycolumnname='weekday')
    jun_weekday_momo_count = jun_weekday_momo_count.to_dict('r')
    json_data = JsonResponse(jun_weekday_momo_count, safe=False)
    return json_data


def jun_weekday_momo_revenue(request):
    jun_weekday_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_weekday_momo_revenue'})
    jun_weekday_momo_revenue = Sort_Dataframeby_Weekday(df=jun_weekday_momo_revenue,Weekdaycolumnname='weekday')
    jun_weekday_momo_revenue = jun_weekday_momo_revenue.to_dict('r')
    json_data = JsonResponse(jun_weekday_momo_revenue, safe=False)
    return json_data


def jun_daily_momo_count(request):
    jun_daily_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_daily_momo_count'})
    jun_daily_momo_count = jun_daily_momo_count.to_dict('r')
    json_data = JsonResponse(jun_daily_momo_count, safe=False)
    return json_data


def jun_daily_momo_revenue(request):
    jun_daily_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_daily_momo_revenue'})
    jun_daily_momo_revenue = jun_daily_momo_revenue.to_dict('r')
    json_data = JsonResponse(jun_daily_momo_revenue, safe=False)
    return json_data


def jun_hourly_momo_count(request):
    jun_hourly_momo_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_hourly_momo_count'})
    jun_hourly_momo_count = jun_hourly_momo_count.to_dict('r')
    json_data = JsonResponse(jun_hourly_momo_count, safe=False)
    return json_data


def jun_hourly_momo_revenue(request):
    jun_hourly_momo_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Momo Interoperability']) & df['month'].isin(['June']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_hourly_momo_revenue'})
    jun_hourly_momo_revenue = jun_hourly_momo_revenue.to_dict('r')
    json_data = JsonResponse(jun_hourly_momo_revenue, safe=False)
    return json_data


def monthly_ft_count(request):
    monthly_ft_count = pd.pivot_table(data=df[df['name'] == 'Funds Transfer'], index='month', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'monthly_ft_count'})
    monthly_ft_count = Sort_Dataframeby_Month(df=monthly_ft_count, monthcolumnname='month')
    monthly_ft_count = monthly_ft_count.to_dict('r')
    json_data = JsonResponse(monthly_ft_count, safe=False)
    return json_data


def monthly_ft_revenue(request):
    monthly_ft_revenue = pd.pivot_table(data=df[df['name'] == 'Funds Transfer'], index='month', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'monthly_ft_revenue'})
    monthly_ft_revenue = Sort_Dataframeby_Month(df=monthly_ft_revenue, monthcolumnname='month')
    monthly_ft_revenue = monthly_ft_revenue.to_dict('r')
    json_data = JsonResponse(monthly_ft_revenue, safe=False)
    return json_data


def weekday_ft_count(request):
    weekday_ft_count = pd.pivot_table(data=df[df['name'] == 'Funds Transfer'], index='weekday', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'weekday_ft_count'})
    weekday_ft_count = Sort_Dataframeby_Weekday(df=weekday_ft_count,Weekdaycolumnname='weekday')
    weekday_ft_count = weekday_ft_count.to_dict('r')
    json_data = JsonResponse(weekday_ft_count, safe=False)
    return json_data


def weekday_ft_revenue(request):
    weekday_ft_revenue = pd.pivot_table(data=df[df['name'] == 'Funds Transfer'], index='weekday', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'weekday_ft_revenue'})
    weekday_ft_revenue = Sort_Dataframeby_Weekday(df=weekday_ft_revenue,Weekdaycolumnname='weekday')
    weekday_ft_revenue = weekday_ft_revenue.to_dict('r')
    json_data = JsonResponse(weekday_ft_revenue, safe=False)
    return json_data


def daily_ft_count(request):
    daily_ft_count = pd.pivot_table(data=df[df['name'] == 'Funds Transfer'], index='day', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'daily_ft_count'})
    daily_ft_count = daily_ft_count.to_dict('r')
    json_data = JsonResponse(daily_ft_count, safe=False)
    return json_data


def daily_ft_revenue(request):
    daily_ft_revenue = pd.pivot_table(data=df[df['name'] == 'Funds Transfer'], index='day', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'daily_ft_revenue'})
    daily_ft_revenue = daily_ft_revenue.to_dict('r')
    json_data = JsonResponse(daily_ft_revenue, safe=False)
    return json_data


def hourly_ft_count(request):
    hourly_ft_count = pd.pivot_table(data=df[df['name'] == 'Funds Transfer'], index='hour', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'hourly_ft_count'})
    hourly_ft_count = hourly_ft_count.to_dict('r')
    json_data = JsonResponse(hourly_ft_count, safe=False)
    return json_data


def hourly_ft_revenue(request):
    hourly_ft_revenue = pd.pivot_table(data=df[df['name'] == 'Funds Transfer'], index='hour', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'hourly_ft_revenue'})
    hourly_ft_revenue = hourly_ft_revenue.to_dict('r')
    json_data = JsonResponse(hourly_ft_revenue, safe=False)
    return json_data


def jan_weekday_ft_count(request):
    jan_weekday_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_weekday_ft_count'})
    jan_weekday_ft_count = Sort_Dataframeby_Weekday(df=jan_weekday_ft_count,Weekdaycolumnname='weekday')
    jan_weekday_ft_count = jan_weekday_ft_count.to_dict('r')
    json_data = JsonResponse(jan_weekday_ft_count, safe=False)
    return json_data


def jan_weekday_ft_revenue(request):
    jan_weekday_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_weekday_ft_revenue'})
    jan_weekday_ft_revenue = Sort_Dataframeby_Weekday(df=jan_weekday_ft_revenue,Weekdaycolumnname='weekday')
    jan_weekday_ft_revenue = jan_weekday_ft_revenue.to_dict('r')
    json_data = JsonResponse(jan_weekday_ft_revenue, safe=False)
    return json_data


def jan_daily_ft_count(request):
    jan_daily_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_daily_ft_count'})
    jan_daily_ft_count = jan_daily_ft_count.to_dict('r')
    json_data = JsonResponse(jan_daily_ft_count, safe=False)
    return json_data


def jan_daily_ft_revenue(request):
    jan_daily_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_daily_ft_revenue'})
    jan_daily_ft_revenue = jan_daily_ft_revenue.to_dict('r')
    json_data = JsonResponse(jan_daily_ft_revenue, safe=False)
    return json_data


def jan_hourly_ft_count(request):
    jan_hourly_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_hourly_ft_count'})
    jan_hourly_ft_count = jan_hourly_ft_count.to_dict('r')
    json_data = JsonResponse(jan_hourly_ft_count, safe=False)
    return json_data


def jan_hourly_ft_revenue(request):
    jan_hourly_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['January']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_hourly_ft_revenue'})
    jan_hourly_ft_revenue = jan_hourly_ft_revenue.to_dict('r')
    json_data = JsonResponse(jan_hourly_ft_revenue, safe=False)
    return json_data


def feb_weekday_ft_count(request):
    feb_weekday_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_weekday_ft_count'})
    feb_weekday_ft_count = Sort_Dataframeby_Weekday(df=feb_weekday_ft_count,Weekdaycolumnname='weekday')
    feb_weekday_ft_count = feb_weekday_ft_count.to_dict('r')
    json_data = JsonResponse(feb_weekday_ft_count, safe=False)
    return json_data


def feb_weekday_ft_revenue(request):
    feb_weekday_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_weekday_ft_revenue'})
    feb_weekday_ft_revenue = Sort_Dataframeby_Weekday(df=feb_weekday_ft_revenue,Weekdaycolumnname='weekday')
    feb_weekday_ft_revenue = feb_weekday_ft_revenue.to_dict('r')
    json_data = JsonResponse(feb_weekday_ft_revenue, safe=False)
    return json_data


def feb_daily_ft_count(request):
    feb_daily_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_daily_ft_count'})
    feb_daily_ft_count = feb_daily_ft_count.to_dict('r')
    json_data = JsonResponse(feb_daily_ft_count, safe=False)
    return json_data


def feb_daily_ft_revenue(request):
    feb_daily_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_daily_ft_revenue'})
    feb_daily_ft_revenue = feb_daily_ft_revenue.to_dict('r')
    json_data = JsonResponse(feb_daily_ft_revenue, safe=False)
    return json_data


def feb_hourly_ft_count(request):
    feb_hourly_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_hourly_ft_count'})
    feb_hourly_ft_count = feb_hourly_ft_count.to_dict('r')
    json_data = JsonResponse(feb_hourly_ft_count, safe=False)
    return json_data


def feb_hourly_ft_revenue(request):
    feb_hourly_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['February']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_hourly_ft_revenue'})
    feb_hourly_ft_revenue = feb_hourly_ft_revenue.to_dict('r')
    json_data = JsonResponse(feb_hourly_ft_revenue, safe=False)
    return json_data


def mar_weekday_ft_count(request):
    mar_weekday_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_weekday_ft_count'})
    mar_weekday_ft_count = Sort_Dataframeby_Weekday(df=mar_weekday_ft_count,Weekdaycolumnname='weekday')
    mar_weekday_ft_count = mar_weekday_ft_count.to_dict('r')
    json_data = JsonResponse(mar_weekday_ft_count, safe=False)
    return json_data


def mar_weekday_ft_revenue(request):
    mar_weekday_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_weekday_ft_revenue'})
    mar_weekday_ft_revenue = Sort_Dataframeby_Weekday(df=mar_weekday_ft_revenue,Weekdaycolumnname='weekday')
    mar_weekday_ft_revenue = mar_weekday_ft_revenue.to_dict('r')
    json_data = JsonResponse(mar_weekday_ft_revenue, safe=False)
    return json_data


def mar_daily_ft_count(request):
    mar_daily_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_daily_ft_count'})
    mar_daily_ft_count = mar_daily_ft_count.to_dict('r')
    json_data = JsonResponse(mar_daily_ft_count, safe=False)
    return json_data


def mar_daily_ft_revenue(request):
    mar_daily_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_daily_ft_revenue'})
    mar_daily_ft_revenue = mar_daily_ft_revenue.to_dict('r')
    json_data = JsonResponse(mar_daily_ft_revenue, safe=False)
    return json_data


def mar_hourly_ft_count(request):
    mar_hourly_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_hourly_ft_count'})
    mar_hourly_ft_count = mar_hourly_ft_count.to_dict('r')
    json_data = JsonResponse(mar_hourly_ft_count, safe=False)
    return json_data


def mar_hourly_ft_revenue(request):
    mar_hourly_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['March']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_hourly_ft_revenue'})
    mar_hourly_ft_revenue = mar_hourly_ft_revenue.to_dict('r')
    json_data = JsonResponse(mar_hourly_ft_revenue, safe=False)
    return json_data


def apr_weekday_ft_count(request):
    apr_weekday_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_weekday_ft_count'})
    apr_weekday_ft_count = Sort_Dataframeby_Weekday(df=apr_weekday_ft_count,Weekdaycolumnname='weekday')
    apr_weekday_ft_count = apr_weekday_ft_count.to_dict('r')
    json_data = JsonResponse(apr_weekday_ft_count, safe=False)
    return json_data


def apr_weekday_ft_revenue(request):
    apr_weekday_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_weekday_ft_revenue'})
    apr_weekday_ft_revenue = Sort_Dataframeby_Weekday(df=apr_weekday_ft_revenue,Weekdaycolumnname='weekday')
    apr_weekday_ft_revenue = apr_weekday_ft_revenue.to_dict('r')
    json_data = JsonResponse(apr_weekday_ft_revenue, safe=False)
    return json_data


def apr_daily_ft_count(request):
    apr_daily_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_daily_ft_count'})
    apr_daily_ft_count = apr_daily_ft_count.to_dict('r')
    json_data = JsonResponse(apr_daily_ft_count, safe=False)
    return json_data


def apr_daily_ft_revenue(request):
    apr_daily_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_daily_ft_revenue'})
    apr_daily_ft_revenue = apr_daily_ft_revenue.to_dict('r')
    json_data = JsonResponse(apr_daily_ft_revenue, safe=False)
    return json_data


def apr_hourly_ft_count(request):
    apr_hourly_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_hourly_ft_count'})
    apr_hourly_ft_count = apr_hourly_ft_count.to_dict('r')
    json_data = JsonResponse(apr_hourly_ft_count, safe=False)
    return json_data


def apr_hourly_ft_revenue(request):
    apr_hourly_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['April']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_hourly_momo_sum'})
    apr_hourly_ft_revenue = apr_hourly_ft_revenue.to_dict('r')
    json_data = JsonResponse(apr_hourly_ft_revenue, safe=False)
    return json_data


def may_weekday_ft_count(request):
    may_weekday_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_weekday_ft_count'})

    may_weekday_ft_count = Sort_Dataframeby_Weekday(df=may_weekday_ft_count,Weekdaycolumnname='weekday')
    may_weekday_ft_count = may_weekday_ft_count.to_dict('r')
    json_data = JsonResponse(may_weekday_ft_count, safe=False)
    return json_data


def may_weekday_ft_revenue(request):
    may_weekday_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_weekday_ft_revenue'})
    may_weekday_ft_revenue = Sort_Dataframeby_Weekday(df=may_weekday_ft_revenue,Weekdaycolumnname='weekday')
    may_weekday_ft_revenue = may_weekday_ft_revenue.to_dict('r')
    json_data = JsonResponse(may_weekday_ft_revenue, safe=False)
    return json_data


def may_daily_ft_count(request):
    may_daily_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_daily_ft_count'})
    may_daily_ft_count = may_daily_ft_count.to_dict('r')
    json_data = JsonResponse(may_daily_ft_count, safe=False)
    return json_data


def may_daily_ft_revenue(request):
    may_daily_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_daily_ft_revenue'})
    may_daily_ft_revenue = may_daily_ft_revenue.to_dict('r')
    json_data = JsonResponse(may_daily_ft_revenue, safe=False)
    return json_data


def may_hourly_ft_count(request):
    may_hourly_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_hourly_ft_count'})
    may_hourly_ft_count = may_hourly_ft_count.to_dict('r')
    json_data = JsonResponse(may_hourly_ft_count, safe=False)
    return json_data


def may_hourly_ft_revenue(request):
    may_hourly_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['May']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_hourly_ft_revenue'})
    may_hourly_ft_revenue = may_hourly_ft_revenue.to_dict('r')
    json_data = JsonResponse(may_hourly_ft_revenue, safe=False)
    return json_data


def jun_weekday_ft_count(request):
    jun_weekday_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_weekday_ft_count'})
    jun_weekday_ft_count = Sort_Dataframeby_Weekday(df=jun_weekday_ft_count,Weekdaycolumnname='weekday')
    jun_weekday_ft_count = jun_weekday_ft_count.to_dict('r')
    json_data = JsonResponse(jun_weekday_ft_count, safe=False)
    return json_data


def jun_weekday_ft_revenue(request):
    jun_weekday_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_weekday_ft_revenue'})
    jun_weekday_ft_revenue = Sort_Dataframeby_Weekday(df=jun_weekday_ft_revenue,Weekdaycolumnname='weekday')
    jun_weekday_ft_revenue = jun_weekday_ft_revenue.to_dict('r')
    json_data = JsonResponse(jun_weekday_ft_revenue, safe=False)
    return json_data


def jun_daily_ft_count(request):
    jun_daily_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_daily_ft_count'})
    jun_daily_ft_count = jun_daily_ft_count.to_dict('r')
    json_data = JsonResponse(jun_daily_ft_count, safe=False)
    return json_data


def jun_daily_ft_revenue(request):
    jun_daily_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_daily_ft_revenue'})
    jun_daily_ft_revenue = jun_daily_ft_revenue.to_dict('r')
    json_data = JsonResponse(jun_daily_ft_revenue, safe=False)
    return json_data


def jun_hourly_ft_count(request):
    jun_hourly_ft_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_hourly_ft_count'})
    jun_hourly_ft_count = jun_hourly_ft_count.to_dict('r')
    json_data = JsonResponse(jun_hourly_ft_count, safe=False)
    return json_data


def jun_hourly_ft_revenue(request):
    jun_hourly_ft_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Funds Transfer']) & df['month'].isin(['June']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_hourly_ft_revenue'})
    jun_hourly_ft_revenue = jun_hourly_ft_revenue.to_dict('r')
    json_data = JsonResponse(jun_hourly_ft_revenue, safe=False)
    return json_data

def monthly_multi_count(request):
    monthly_multi_count = pd.pivot_table(data=df[df['name'] == 'Multichoice Payment'], index='month', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'monthly_multi_count'})
    monthly_multi_count = Sort_Dataframeby_Month(df=monthly_multi_count, monthcolumnname='month')
    monthly_multi_count = monthly_multi_count.to_dict('r')
    json_data = JsonResponse(monthly_multi_count, safe=False)
    return json_data


def monthly_multi_revenue(request):
    monthly_multi_revenue = pd.pivot_table(data=df[df['name'] == 'Multichoice Payment'], index='month', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'monthly_multi_revenue'})
    monthly_multi_revenue = Sort_Dataframeby_Month(df=monthly_multi_revenue, monthcolumnname='month')
    monthly_multi_revenue = monthly_multi_revenue.to_dict('r')
    json_data = JsonResponse(monthly_multi_revenue, safe=False)
    return json_data


def weekday_multi_count(request):
    weekday_multi_count = pd.pivot_table(data=df[df['name'] == 'Multichoice Payment'], index='weekday', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'weekday_multi_count'})
    weekday_multi_count = Sort_Dataframeby_Weekday(df=weekday_multi_count,Weekdaycolumnname='weekday')
    weekday_multi_count = weekday_multi_count.to_dict('r')
    json_data = JsonResponse(weekday_multi_count, safe=False)
    return json_data


def weekday_multi_revenue(request):
    weekday_multi_revenue = pd.pivot_table(data=df[df['name'] == 'Multichoice Payment'], index='weekday', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'weekday_multi_revenue'})
    weekday_multi_revenue = Sort_Dataframeby_Weekday(df=weekday_multi_revenue,Weekdaycolumnname='weekday')
    weekday_multi_revenue = weekday_multi_revenue.to_dict('r')
    json_data = JsonResponse(weekday_multi_revenue, safe=False)
    return json_data


def daily_multi_count(request):
    daily_multi_revenue = pd.pivot_table(data=df[df['name'] == 'Multichoice Payment'], index='day', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'daily_multi_revenue'})
    daily_multi_revenue = daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(daily_multi_revenue, safe=False)
    return json_data


def daily_multi_revenue(request):
    daily_multi_revenue = pd.pivot_table(data=df[df['name'] == 'Multichoice Payment'], index='day', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'daily_multi_revenue'})
    daily_multi_revenue = daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(daily_multi_revenue, safe=False)
    return json_data


def hourly_multi_count(request):
    hourly_multi_count = pd.pivot_table(data=df[df['name'] == 'Multichoice Payment'], index='hour', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'hourly_multi_count'})
    hourly_multi_count = hourly_multi_count.to_dict('r')
    json_data = JsonResponse(hourly_multi_count, safe=False)
    return json_data


def hourly_multi_revenue(request):
    hourly_multi_revenue = pd.pivot_table(data=df[df['name'] == 'Multichoice Payment'], index='hour', values='amount',
                                           aggfunc='sum').reset_index().rename(
        columns={'amount': 'hourly_multi_revenue'})
    hourly_multi_revenue = hourly_multi_revenue.to_dict('r')
    json_data = JsonResponse(hourly_multi_revenue, safe=False)
    return json_data


def jan_weekday_multi_count(request):
    jan_weekday_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_weekday_multi_count'})
    jan_weekday_multi_count = Sort_Dataframeby_Weekday(df=jan_weekday_multi_count,Weekdaycolumnname='weekday')
    jan_weekday_multi_count = jan_weekday_multi_count.to_dict('r')
    json_data = JsonResponse(jan_weekday_multi_count, safe=False)
    return json_data


def jan_weekday_multi_revenue(request):
    jan_weekday_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_weekday_multi_revenue'})
    jan_weekday_multi_revenue = Sort_Dataframeby_Weekday(df=jan_weekday_multi_revenue,Weekdaycolumnname='weekday')
    jan_weekday_multi_revenue = jan_weekday_multi_revenue.to_dict('r')
    json_data = JsonResponse(jan_weekday_multi_revenue, safe=False)
    return json_data


def jan_daily_multi_count(request):
    jan_daily_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_daily_multi_count'})
    jan_daily_multi_count = jan_daily_multi_count.to_dict('r')
    json_data = JsonResponse(jan_daily_multi_count, safe=False)
    return json_data


def jan_daily_multi_revenue(request):
    jan_daily_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_daily_multi_revenue'})
    jan_daily_multi_revenue = jan_daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(jan_daily_multi_revenue, safe=False)
    return json_data


def jan_hourly_multi_count(request):
    jan_hourly_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_hourly_multi_count'})
    jan_hourly_multi_count = jan_hourly_multi_count.to_dict('r')
    json_data = JsonResponse(jan_hourly_multi_count, safe=False)
    return json_data


def jan_hourly_multi_revenue(request):
    jan_hourly_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['January']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jan_hourly_multi_revenue'})
    jan_hourly_multi_revenue = jan_hourly_multi_revenue.to_dict('r')
    json_data = JsonResponse(jan_hourly_multi_revenue, safe=False)
    return json_data


def feb_weekday_multi_count(request):
    feb_weekday_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_weekday_multi_count'})
    feb_weekday_multi_count = Sort_Dataframeby_Weekday(df=feb_weekday_multi_count,Weekdaycolumnname='weekday')
    feb_weekday_multi_count = feb_weekday_multi_count.to_dict('r')
    json_data = JsonResponse(feb_weekday_multi_count, safe=False)
    return json_data


def feb_weekday_multi_revenue(request):
    feb_weekday_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_weekday_multi_revenue'})
    feb_weekday_multi_revenue = Sort_Dataframeby_Weekday(df=feb_weekday_multi_revenue,Weekdaycolumnname='weekday')
    feb_weekday_multi_revenue = feb_weekday_multi_revenue.to_dict('r')
    json_data = JsonResponse(feb_weekday_multi_revenue, safe=False)
    return json_data


def feb_daily_multi_count(request):
    feb_daily_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_daily_multi_count'})
    feb_daily_multi_count = feb_daily_multi_count.to_dict('r')
    json_data = JsonResponse(feb_daily_multi_count, safe=False)
    return json_data


def feb_daily_multi_revenue(request):
    feb_daily_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_daily_multi_revenue'})
    feb_daily_multi_revenue = feb_daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(feb_daily_multi_revenue, safe=False)
    return json_data


def feb_hourly_multi_count(request):
    feb_hourly_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_hourly_multi_count'})
    feb_hourly_multi_count = feb_hourly_multi_count.to_dict('r')
    json_data = JsonResponse(feb_hourly_multi_count, safe=False)
    return json_data


def feb_hourly_multi_revenue(request):
    feb_hourly_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['February']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'feb_hourly_multi_revenue'})
    feb_hourly_multi_revenue = feb_hourly_multi_revenue.to_dict('r')
    json_data = JsonResponse(feb_hourly_multi_revenue, safe=False)
    return json_data


def mar_weekday_multi_count(request):
    mar_weekday_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_weekday_multi_count'})
    mar_weekday_multi_count = Sort_Dataframeby_Weekday(df=mar_weekday_multi_count,Weekdaycolumnname='weekday')
    mar_weekday_multi_count = mar_weekday_multi_count.to_dict('r')
    json_data = JsonResponse(mar_weekday_multi_count, safe=False)
    return json_data


def mar_weekday_multi_revenue(request):
    mar_weekday_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_weekday_multi_revenue'})
    mar_weekday_multi_revenue = Sort_Dataframeby_Weekday(df=mar_weekday_multi_revenue,Weekdaycolumnname='weekday')
    mar_weekday_multi_revenue = mar_weekday_multi_revenue.to_dict('r')
    json_data = JsonResponse(mar_weekday_multi_revenue, safe=False)
    return json_data


def mar_daily_multi_count(request):
    mar_daily_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_daily_multi_count'})
    mar_daily_multi_count = mar_daily_multi_count.to_dict('r')
    json_data = JsonResponse(mar_daily_multi_count, safe=False)
    return json_data


def mar_daily_multi_revenue(request):
    mar_daily_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_daily_multi_revenue'})
    mar_daily_multi_revenue = mar_daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(mar_daily_multi_revenue, safe=False)
    return json_data


def mar_hourly_multi_count(request):
    mar_hourly_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_hourly_multi_count'})
    mar_hourly_multi_count = mar_hourly_multi_count.to_dict('r')
    json_data = JsonResponse(mar_hourly_multi_count, safe=False)
    return json_data


def mar_hourly_multi_revenue(request):
    mar_hourly_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['March']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'mar_hourly_multi_revenue'})
    mar_hourly_multi_revenue = mar_hourly_multi_revenue.to_dict('r')
    json_data = JsonResponse(mar_hourly_multi_revenue, safe=False)
    return json_data


def apr_weekday_multi_count(request):
    apr_weekday_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_weekday_multi_count'})
    apr_weekday_multi_count = Sort_Dataframeby_Weekday(df=apr_weekday_multi_count,Weekdaycolumnname='weekday')
    apr_weekday_multi_count = apr_weekday_multi_count.to_dict('r')
    json_data = JsonResponse(apr_weekday_multi_count, safe=False)
    return json_data


def apr_weekday_multi_revenue(request):
    apr_weekday_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_weekday_multi_revenue'})
    apr_weekday_multi_revenue = Sort_Dataframeby_Weekday(df=apr_weekday_multi_revenue,Weekdaycolumnname='weekday')
    apr_weekday_multi_revenue = apr_weekday_multi_revenue.to_dict('r')
    json_data = JsonResponse(apr_weekday_multi_revenue, safe=False)
    return json_data


def apr_daily_multi_count(request):
    apr_daily_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_daily_multi_count'})
    apr_daily_multi_count = apr_daily_multi_count.to_dict('r')
    json_data = JsonResponse(apr_daily_multi_count, safe=False)
    return json_data


def apr_daily_multi_revenue(request):
    apr_daily_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_daily_multi_revenue'})
    apr_daily_multi_revenue = apr_daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(apr_daily_multi_revenue, safe=False)
    return json_data


def apr_hourly_multi_count(request):
    apr_hourly_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_hourly_multi_count'})
    apr_hourly_multi_count = apr_hourly_multi_count.to_dict('r')
    json_data = JsonResponse(apr_hourly_multi_count, safe=False)
    return json_data


def apr_hourly_multi_revenue(request):
    apr_hourly_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['April']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'apr_hourly_multi_revenue'})
    apr_hourly_multi_revenue = apr_hourly_multi_revenue.to_dict('r')
    json_data = JsonResponse(apr_hourly_multi_revenue, safe=False)
    return json_data


def may_weekday_multi_count(request):
    may_weekday_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_weekday_multi_count'})
    may_weekday_multi_count = Sort_Dataframeby_Weekday(df=may_weekday_multi_count,Weekdaycolumnname='weekday')
    may_weekday_multi_count = may_weekday_multi_count.to_dict('r')
    json_data = JsonResponse(may_weekday_multi_count, safe=False)
    return json_data


def may_weekday_multi_revenue(request):
    may_weekday_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_weekday_multi_revenue'})
    may_weekday_multi_revenue = Sort_Dataframeby_Weekday(df=may_weekday_multi_revenue,Weekdaycolumnname='weekday')
    may_weekday_multi_revenue = may_weekday_multi_revenue.to_dict('r')
    json_data = JsonResponse(may_weekday_multi_revenue, safe=False)
    return json_data


def may_daily_multi_count(request):
    may_daily_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_daily_multi_count'})
    may_daily_multi_count = may_daily_multi_count.to_dict('r')
    json_data = JsonResponse(may_daily_multi_count, safe=False)
    return json_data


def may_daily_multi_revenue(request):
    may_daily_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_daily_multi_revenue'})
    may_daily_multi_revenue = may_daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(may_daily_multi_revenue, safe=False)
    return json_data


def may_hourly_multi_count(request):
    may_hourly_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_hourly_multi_count'})
    may_hourly_multi_count = may_hourly_multi_count.to_dict('r')
    json_data = JsonResponse(may_hourly_multi_count, safe=False)
    return json_data


def may_hourly_multi_revenue(request):
    may_hourly_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['May']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'may_hourly_multi_revenue'})
    may_hourly_multi_revenue = may_hourly_multi_revenue.to_dict('r')
    json_data = JsonResponse(may_hourly_multi_revenue, safe=False)
    return json_data


def jun_weekday_multi_count(request):
    jun_weekday_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_weekday_multi_count'})
    jun_weekday_multi_count = Sort_Dataframeby_Weekday(df=jun_weekday_multi_count,Weekdaycolumnname='weekday')
    jun_weekday_multi_count = jun_weekday_multi_count.to_dict('r')
    json_data = JsonResponse(jun_weekday_multi_count, safe=False)
    return json_data


def jun_weekday_multi_revenue(request):
    jun_weekday_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_weekday_multi_revenue'})
    jun_weekday_multi_revenue = Sort_Dataframeby_Weekday(df=jun_weekday_multi_revenue,Weekdaycolumnname='weekday')
    jun_weekday_multi_revenue = jun_weekday_multi_revenue.to_dict('r')
    json_data = JsonResponse(jun_weekday_multi_revenue, safe=False)
    return json_data


def jun_daily_multi_count(request):
    jun_daily_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_daily_multi_count'})
    jun_daily_multi_count = jun_daily_multi_count.to_dict('r')
    json_data = JsonResponse(jun_daily_multi_count, safe=False)
    return json_data


def jun_daily_multi_revenue(request):
    jun_daily_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_daily_multi_revenue'})
    jun_daily_multi_revenue = jun_daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(jun_daily_multi_revenue, safe=False)
    return json_data


def jun_hourly_multi_count(request):
    jun_hourly_multi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_hourly_multi_count'})
    jun_hourly_multi_count = jun_hourly_multi_count.to_dict('r')
    json_data = JsonResponse(jun_hourly_multi_count, safe=False)
    return json_data


def jun_hourly_multi_revenue(request):
    jun_hourly_multi_revenue = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Multichoice Payment']) & df['month'].isin(['June']))], index='hour',
        values='amount',
        aggfunc='sum').reset_index().rename(columns={'amount': 'jun_hourly_multi_revenue'})
    jun_hourly_multi_revenue = jun_hourly_multi_revenue.to_dict('r')
    json_data = JsonResponse(jun_hourly_multi_revenue, safe=False)
    return json_data

def monthly_bi_count(request):
    monthly_bi_count = pd.pivot_table(data=df[df['name'] == 'Balance Inquiry'], index='month', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'monthly_momo_count'})
    monthly_bi_count = Sort_Dataframeby_Month(df=monthly_bi_count, monthcolumnname='month')
    monthly_bi_count = monthly_bi_count.to_dict('r')
    json_data = JsonResponse(monthly_bi_count, safe=False)
    return json_data


def weekday_bi_count(request):
    weekday_bi_count = pd.pivot_table(data=df[df['name'] == 'Balance Inquiry'], index='weekday', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'weekday_bi_count'})
    weekday_bi_count = Sort_Dataframeby_Weekday(df=weekday_bi_count,Weekdaycolumnname='weekday')
    weekday_bi_count = weekday_bi_count.to_dict('r')
    json_data = JsonResponse(weekday_bi_count, safe=False)
    return json_data


def daily_bi_count(request):
    daily_multi_revenue = pd.pivot_table(data=df[df['name'] == 'Balance Inquiry'], index='day', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'daily_multi_revenue'})
    daily_multi_revenue = daily_multi_revenue.to_dict('r')
    json_data = JsonResponse(daily_multi_revenue, safe=False)
    return json_data


def hourly_bi_count(request):
    hourly_bi_count = pd.pivot_table(data=df[df['name'] == 'Balance Inquiry'], index='hour', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'hourly_bi_count'})
    hourly_bi_count = hourly_bi_count.to_dict('r')
    json_data = JsonResponse(hourly_bi_count, safe=False)
    return json_data


def jan_weekday_bi_count(request):
    jan_weekday_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_weekday_momo_count'})
    jan_weekday_bi_count = Sort_Dataframeby_Weekday(df=jan_weekday_bi_count,Weekdaycolumnname='weekday')
    jan_weekday_bi_count = jan_weekday_bi_count.to_dict('r')
    json_data = JsonResponse(jan_weekday_bi_count, safe=False)
    return json_data


def jan_daily_bi_count(request):
    jan_daily_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_daily_bi_count'})
    jan_daily_bi_count = jan_daily_bi_count.to_dict('r')
    json_data = JsonResponse(jan_daily_bi_count, safe=False)
    return json_data


def jan_hourly_bi_count(request):
    jan_hourly_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_hourly_bi_count'})
    jan_hourly_bi_count = jan_hourly_bi_count.to_dict('r')
    json_data = JsonResponse(jan_hourly_bi_count, safe=False)
    return json_data


def feb_weekday_bi_count(request):
    feb_weekday_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_weekday_bi_count'})
    feb_weekday_bi_count = Sort_Dataframeby_Weekday(df=feb_weekday_bi_count,Weekdaycolumnname='weekday')
    feb_weekday_bi_count = feb_weekday_bi_count.to_dict('r')
    json_data = JsonResponse(feb_weekday_bi_count, safe=False)
    return json_data


def feb_daily_bi_count(request):
    feb_daily_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_daily_bi_count'})
    feb_daily_bi_count = feb_daily_bi_count.to_dict('r')
    json_data = JsonResponse(feb_daily_bi_count, safe=False)
    return json_data


def feb_hourly_bi_count(request):
    feb_hourly_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_hourly_bi_count'})
    feb_hourly_bi_count = feb_hourly_bi_count.to_dict('r')
    json_data = JsonResponse(feb_hourly_bi_count, safe=False)
    return json_data


def mar_weekday_bi_count(request):
    mar_weekday_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_weekday_bi_count'})
    mar_weekday_bi_count = Sort_Dataframeby_Weekday(df=mar_weekday_bi_count,Weekdaycolumnname='weekday')
    mar_weekday_bi_count = mar_weekday_bi_count.to_dict('r')
    json_data = JsonResponse(mar_weekday_bi_count, safe=False)
    return json_data


def mar_daily_bi_count(request):
    mar_daily_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_daily_bi_count'})
    mar_daily_bi_count = mar_daily_bi_count.to_dict('r')
    json_data = JsonResponse(mar_daily_bi_count, safe=False)
    return json_data


def mar_hourly_bi_count(request):
    mar_hourly_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_hourly_bi_count'})
    mar_hourly_bi_count = mar_hourly_bi_count.to_dict('r')
    json_data = JsonResponse(mar_hourly_bi_count, safe=False)
    return json_data


def apr_weekday_bi_count(request):
    apr_weekday_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_weekday_bi_count'})
    apr_weekday_bi_count = Sort_Dataframeby_Weekday(df=apr_weekday_bi_count,Weekdaycolumnname='weekday')
    apr_weekday_bi_count = apr_weekday_bi_count.to_dict('r')
    json_data = JsonResponse(apr_weekday_bi_count, safe=False)
    return json_data


def apr_daily_bi_count(request):
    apr_daily_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_daily_bi_count'})
    apr_daily_bi_count = apr_daily_bi_count.to_dict('r')
    json_data = JsonResponse(apr_daily_bi_count, safe=False)
    return json_data


def apr_hourly_bi_count(request):
    apr_hourly_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_hourly_bi_count'})
    apr_hourly_bi_count = apr_hourly_bi_count.to_dict('r')
    json_data = JsonResponse(apr_hourly_bi_count, safe=False)
    return json_data


def may_weekday_bi_count(request):
    may_weekday_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_weekday_bi_count'})
    may_weekday_bi_count = Sort_Dataframeby_Weekday(df=may_weekday_bi_count,Weekdaycolumnname='weekday')
    may_weekday_bi_count = may_weekday_bi_count.to_dict('r')
    json_data = JsonResponse(may_weekday_bi_count, safe=False)
    return json_data


def may_daily_bi_count(request):
    may_daily_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_daily_bi_count'})
    may_daily_bi_count = may_daily_bi_count.to_dict('r')
    json_data = JsonResponse(may_daily_bi_count, safe=False)
    return json_data


def may_hourly_bi_count(request):
    may_hourly_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_hourly_bi_count'})
    may_hourly_bi_count = may_hourly_bi_count.to_dict('r')
    json_data = JsonResponse(may_hourly_bi_count, safe=False)
    return json_data


def jun_weekday_bi_count(request):
    jun_weekday_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_weekday_bi_count'})
    jun_weekday_bi_count = Sort_Dataframeby_Weekday(df=jun_weekday_bi_count,Weekdaycolumnname='weekday')
    jun_weekday_bi_count = jun_weekday_bi_count.to_dict('r')
    json_data = JsonResponse(jun_weekday_bi_count, safe=False)
    return json_data


def jun_daily_bi_count(request):
    jun_daily_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_daily_bi_count'})
    jun_daily_bi_count = jun_daily_bi_count.to_dict('r')
    json_data = JsonResponse(jun_daily_bi_count, safe=False)
    return json_data


def jun_hourly_bi_count(request):
    jun_hourly_bi_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Balance Inquiry']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_hourly_bi_count'})
    jun_hourly_bi_count = jun_hourly_bi_count.to_dict('r')
    json_data = JsonResponse(jun_hourly_bi_count, safe=False)
    return json_data


def monthly_forex_count(request):
    monthly_forex_count = pd.pivot_table(data=df[df['name'] == 'Forex Rates'], index='month', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'monthly_forex_count'})
    monthly_forex_count = Sort_Dataframeby_Month(df=monthly_forex_count, monthcolumnname='month')
    monthly_forex_count = monthly_forex_count.to_dict('r')
    json_data = JsonResponse(monthly_forex_count, safe=False)
    return json_data


def weekday_forex_count(request):
    weekday_forex_count = pd.pivot_table(data=df[df['name'] == 'Forex Rates'], index='weekday', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'weekday_forex_count'})
    weekday_forex_count = Sort_Dataframeby_Weekday(df=weekday_forex_count,Weekdaycolumnname='weekday')
    weekday_forex_count = weekday_forex_count.to_dict('r')
    json_data = JsonResponse(weekday_forex_count, safe=False)
    return json_data


def daily_forex_count(request):
    daily_forex_count = pd.pivot_table(data=df[df['name'] == 'Forex Rates'], index='day', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'daily_forex_count'})
    daily_forex_count = daily_forex_count.to_dict('r')
    json_data = JsonResponse(daily_forex_count, safe=False)
    return json_data


def hourly_forex_count(request):
    hourly_forex_count = pd.pivot_table(data=df[df['name'] == 'Forex Rates'], index='hour', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'hourly_forex_count'})
    hourly_forex_count = hourly_forex_count.to_dict('r')
    json_data = JsonResponse(hourly_forex_count, safe=False)
    return json_data


def jan_weekday_forex_count(request):
    jan_weekday_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_weekday_forex_count'})
    jan_weekday_forex_count = Sort_Dataframeby_Weekday(df=jan_weekday_forex_count,Weekdaycolumnname='weekday')
    jan_weekday_forex_count = jan_weekday_forex_count.to_dict('r')
    json_data = JsonResponse(jan_weekday_forex_count, safe=False)
    return json_data


def jan_daily_forex_count(request):
    jan_daily_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_daily_forex_count'})
    jan_daily_forex_count = jan_daily_forex_count.to_dict('r')
    json_data = JsonResponse(jan_daily_forex_count, safe=False)
    return json_data


def jan_hourly_forex_count(request):
    jan_hourly_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_hourly_forex_count'})
    jan_hourly_forex_count = jan_hourly_forex_count.to_dict('r')
    json_data = JsonResponse(jan_hourly_forex_count, safe=False)
    return json_data


def feb_weekday_forex_count(request):
    feb_weekday_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_weekday_forex_count'})
    feb_weekday_forex_count = Sort_Dataframeby_Weekday(df=feb_weekday_forex_count,Weekdaycolumnname='weekday')
    feb_weekday_forex_count = feb_weekday_forex_count.to_dict('r')
    json_data = JsonResponse(feb_weekday_forex_count, safe=False)
    return json_data


def feb_daily_forex_count(request):
    feb_daily_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_daily_forex_count'})
    feb_daily_forex_count = feb_daily_forex_count.to_dict('r')
    json_data = JsonResponse(feb_daily_forex_count, safe=False)
    return json_data


def feb_hourly_forex_count(request):
    feb_hourly_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_hourly_forex_count'})
    feb_hourly_forex_count = feb_hourly_forex_count.to_dict('r')
    json_data = JsonResponse(feb_hourly_forex_count, safe=False)
    return json_data


def mar_weekday_forex_count(request):
    mar_weekday_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_weekday_forex_count'})
    mar_weekday_forex_count = Sort_Dataframeby_Weekday(df=mar_weekday_forex_count,Weekdaycolumnname='weekday')
    mar_weekday_forex_count = mar_weekday_forex_count.to_dict('r')
    json_data = JsonResponse(mar_weekday_forex_count, safe=False)
    return json_data


def mar_daily_forex_count(request):
    mar_daily_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_daily_forex_count'})
    mar_daily_forex_count = mar_daily_forex_count.to_dict('r')
    json_data = JsonResponse(mar_daily_forex_count, safe=False)
    return json_data


def mar_hourly_forex_count(request):
    mar_hourly_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_hourly_forex_count'})
    mar_hourly_forex_count = mar_hourly_forex_count.to_dict('r')
    json_data = JsonResponse(mar_hourly_forex_count, safe=False)
    return json_data


def apr_weekday_forex_count(request):
    apr_weekday_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_weekday_forex_count'})
    apr_weekday_forex_count = Sort_Dataframeby_Weekday(df=apr_weekday_forex_count,Weekdaycolumnname='weekday')
    apr_weekday_forex_count = apr_weekday_forex_count.to_dict('r')
    json_data = JsonResponse(apr_weekday_forex_count, safe=False)
    return json_data


def apr_daily_forex_count(request):
    apr_daily_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_daily_forex_count'})
    apr_daily_forex_count = apr_daily_forex_count.to_dict('r')
    json_data = JsonResponse(apr_daily_forex_count, safe=False)
    return json_data


def apr_hourly_forex_count(request):
    apr_hourly_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_hourly_forex_count'})
    apr_hourly_forex_count = apr_hourly_forex_count.to_dict('r')
    json_data = JsonResponse(apr_hourly_forex_count, safe=False)
    return json_data


def may_weekday_forex_count(request):
    may_weekday_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_weekday_forex_count'})
    may_weekday_forex_count = Sort_Dataframeby_Weekday(df=may_weekday_forex_count,Weekdaycolumnname='weekday')
    may_weekday_forex_count = may_weekday_forex_count.to_dict('r')
    json_data = JsonResponse(may_weekday_forex_count, safe=False)
    return json_data


def may_daily_forex_count(request):
    may_daily_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_daily_forex_count'})
    may_daily_forex_count = may_daily_forex_count.to_dict('r')
    json_data = JsonResponse(may_daily_forex_count, safe=False)
    return json_data


def may_hourly_forex_count(request):
    may_hourly_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_hourly_forex_count'})
    may_hourly_forex_count = may_hourly_forex_count.to_dict('r')
    json_data = JsonResponse(may_hourly_forex_count, safe=False)
    return json_data


def jun_weekday_forex_count(request):
    jun_weekday_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_weekday_forex_count'})
    jun_weekday_forex_count = Sort_Dataframeby_Weekday(df=jun_weekday_forex_count,Weekdaycolumnname='weekday')
    jun_weekday_forex_count = jun_weekday_forex_count.to_dict('r')
    json_data = JsonResponse(jun_weekday_forex_count, safe=False)
    return json_data


def jun_daily_forex_count(request):
    jun_daily_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_daily_forex_count'})
    jun_daily_forex_count = jun_daily_forex_count.to_dict('r')
    json_data = JsonResponse(jun_daily_forex_count, safe=False)
    return json_data


def jun_hourly_forex_count(request):
    jun_hourly_forex_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Forex Rates']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_hourly_forex_count'})
    jun_hourly_forex_count = jun_hourly_forex_count.to_dict('r')
    json_data = JsonResponse(jun_hourly_forex_count, safe=False)
    return json_data


def monthly_view_prof_count(request):
    monthly_view_prof_count = pd.pivot_table(data=df[df['name'] == 'View_my_profile'], index='month', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'monthly_view_prof_count'})
    monthly_view_prof_count = Sort_Dataframeby_Month(df=monthly_view_prof_count, monthcolumnname='month')
    monthly_view_prof_count = monthly_view_prof_count.to_dict('r')
    json_data = JsonResponse(monthly_view_prof_count, safe=False)
    return json_data


def weekday_view_prof_count(request):
    weekday_view_prof_count = pd.pivot_table(data=df[df['name'] == 'View_my_profile'], index='weekday', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'weekday_view_prof_count'})
    weekday_view_prof_count = Sort_Dataframeby_Weekday(df=weekday_view_prof_count,Weekdaycolumnname='weekday')
    weekday_view_prof_count = weekday_view_prof_count.to_dict('r')
    json_data = JsonResponse(weekday_view_prof_count, safe=False)
    return json_data


def daily_view_prof_count(request):
    daily_view_prof_count = pd.pivot_table(data=df[df['name'] == 'View_my_profile'], index='day', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'daily_view_prof_count'})
    daily_view_prof_count = daily_view_prof_count.to_dict('r')
    json_data = JsonResponse(daily_view_prof_count, safe=False)
    return json_data


def hourly_view_prof_count(request):
    hourly_view_prof_count = pd.pivot_table(data=df[df['name'] == 'View_my_profile'], index='hour', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'hourly_view_prof_count'})
    hourly_view_prof_count = hourly_view_prof_count.to_dict('r')
    json_data = JsonResponse(hourly_view_prof_count, safe=False)
    return json_data


def jan_weekday_view_prof_count(request):
    jan_weekday_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_weekday_view_prof_count'})
    jan_weekday_view_prof_count = Sort_Dataframeby_Weekday(df=jan_weekday_view_prof_count,Weekdaycolumnname='weekday')
    jan_weekday_view_prof_count = jan_weekday_view_prof_count.to_dict('r')
    json_data = JsonResponse(jan_weekday_view_prof_count, safe=False)
    return json_data


def jan_daily_view_prof_count(request):
    jan_daily_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_daily_view_prof_count'})
    jan_daily_view_prof_count = jan_daily_view_prof_count.to_dict('r')
    json_data = JsonResponse(jan_daily_view_prof_count, safe=False)
    return json_data


def jan_hourly_view_prof_count(request):
    jan_hourly_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_hourly_view_prof_count'})
    jan_hourly_view_prof_count = jan_hourly_view_prof_count.to_dict('r')
    json_data = JsonResponse(jan_hourly_view_prof_count, safe=False)
    return json_data


def feb_weekday_view_prof_count(request):
    feb_weekday_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_weekday_view_prof_count'})
    feb_weekday_view_prof_count = Sort_Dataframeby_Weekday(df=feb_weekday_view_prof_count,Weekdaycolumnname='weekday')
    feb_weekday_view_prof_count = feb_weekday_view_prof_count.to_dict('r')
    json_data = JsonResponse(feb_weekday_view_prof_count, safe=False)
    return json_data


def feb_daily_view_prof_count(request):
    feb_daily_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_daily_view_prof_count'})
    feb_daily_view_prof_count = feb_daily_view_prof_count.to_dict('r')
    json_data = JsonResponse(feb_daily_view_prof_count, safe=False)
    return json_data


def feb_hourly_view_prof_count(request):
    feb_hourly_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_hourly_view_prof_count'})
    feb_hourly_view_prof_count = feb_hourly_view_prof_count.to_dict('r')
    json_data = JsonResponse(feb_hourly_view_prof_count, safe=False)
    return json_data


def mar_weekday_view_prof_count(request):
    mar_weekday_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_weekday_view_prof_count'})
    mar_weekday_view_prof_count = Sort_Dataframeby_Weekday(df=mar_weekday_view_prof_count,Weekdaycolumnname='weekday')
    mar_weekday_view_prof_count = mar_weekday_view_prof_count.to_dict('r')
    json_data = JsonResponse(mar_weekday_view_prof_count, safe=False)
    return json_data


def mar_daily_view_prof_count(request):
    mar_daily_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_daily_view_prof_count'})
    mar_daily_view_prof_count = mar_daily_view_prof_count.to_dict('r')
    json_data = JsonResponse(mar_daily_view_prof_count, safe=False)
    return json_data


def mar_hourly_view_prof_count(request):
    mar_hourly_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_hourly_view_prof_count'})
    mar_hourly_view_prof_count = mar_hourly_view_prof_count.to_dict('r')
    json_data = JsonResponse(mar_hourly_view_prof_count, safe=False)
    return json_data


def apr_weekday_view_prof_count(request):
    apr_weekday_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_weekday_view_prof_count'})
    apr_weekday_view_prof_count = Sort_Dataframeby_Weekday(df=apr_weekday_view_prof_count,Weekdaycolumnname='weekday')
    apr_weekday_view_prof_count = apr_weekday_view_prof_count.to_dict('r')
    json_data = JsonResponse(apr_weekday_view_prof_count, safe=False)
    return json_data


def apr_daily_view_prof_count(request):
    apr_daily_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_daily_view_prof_count'})
    apr_daily_view_prof_count = apr_daily_view_prof_count.to_dict('r')
    json_data = JsonResponse(apr_daily_view_prof_count, safe=False)
    return json_data


def apr_hourly_view_prof_count(request):
    apr_hourly_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_hourly_view_prof_count'})
    apr_hourly_view_prof_count = apr_hourly_view_prof_count.to_dict('r')
    json_data = JsonResponse(apr_hourly_view_prof_count, safe=False)
    return json_data


def may_weekday_view_prof_count(request):
    may_weekday_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_weekday_view_prof_count'})
    may_weekday_view_prof_count = Sort_Dataframeby_Weekday(df=may_weekday_view_prof_count,Weekdaycolumnname='weekday')
    may_weekday_view_prof_count = may_weekday_view_prof_count.to_dict('r')
    json_data = JsonResponse(may_weekday_view_prof_count, safe=False)
    return json_data


def may_daily_view_prof_count(request):
    may_daily_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_daily_view_prof_count'})
    may_daily_view_prof_count = may_daily_view_prof_count.to_dict('r')
    json_data = JsonResponse(may_daily_view_prof_count, safe=False)
    return json_data


def may_hourly_view_prof_count(request):
    may_hourly_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_hourly_view_prof_count'})
    may_hourly_view_prof_count = may_hourly_view_prof_count.to_dict('r')
    json_data = JsonResponse(may_hourly_view_prof_count, safe=False)
    return json_data


def jun_weekday_view_prof_count(request):
    jun_weekday_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_weekday_view_prof_count'})
    jun_weekday_view_prof_count = Sort_Dataframeby_Weekday(df=jun_weekday_view_prof_count,Weekdaycolumnname='weekday')
    jun_weekday_view_prof_count = jun_weekday_view_prof_count.to_dict('r')
    json_data = JsonResponse(jun_weekday_view_prof_count, safe=False)
    return json_data


def jun_daily_view_prof_count(request):
    jun_daily_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_daily_view_prof_count'})
    jun_daily_view_prof_count = jun_daily_view_prof_count.to_dict('r')
    json_data = JsonResponse(jun_daily_view_prof_count, safe=False)
    return json_data


def jun_hourly_view_prof_count(request):
    jun_hourly_view_prof_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['View_my_profile']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_hourly_view_prof_count'})
    jun_hourly_view_prof_count = jun_hourly_view_prof_count.to_dict('r')
    json_data = JsonResponse(jun_hourly_view_prof_count, safe=False)
    return json_data


def monthly_reset_pass_count(request):
    monthly_reset_pass_count = pd.pivot_table(data=df[df['name'] == 'Reset_password'], index='month', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'monthly_reset_pass_count'})
    monthly_reset_pass_count = Sort_Dataframeby_Month(df=monthly_reset_pass_count, monthcolumnname='month')
    monthly_reset_pass_count = monthly_reset_pass_count.to_dict('r')
    json_data = JsonResponse(monthly_reset_pass_count, safe=False)
    return json_data


def weekday_reset_pass_count(request):
    weekday_reset_pass_count = pd.pivot_table(data=df[df['name'] == 'Reset_password'], index='weekday', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'weekday_reset_pass_count'})
    weekday_reset_pass_count = Sort_Dataframeby_Weekday(df=weekday_reset_pass_count,Weekdaycolumnname='weekday')
    weekday_reset_pass_count = weekday_reset_pass_count.to_dict('r')
    json_data = JsonResponse(weekday_reset_pass_count, safe=False)
    return json_data


def daily_reset_pass_count(request):
    daily_reset_pass_count = pd.pivot_table(data=df[df['name'] == 'Reset_password'], index='day', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'daily_reset_pass_count'})
    daily_reset_pass_count = daily_reset_pass_count.to_dict('r')
    json_data = JsonResponse(daily_reset_pass_count, safe=False)
    return json_data


def hourly_reset_pass_count(request):
    hourly_reset_pass_count = pd.pivot_table(data=df[df['name'] == 'Reset_password'], index='hour', values='amount',
                                           aggfunc='count').reset_index().rename(
        columns={'amount': 'hourly_reset_pass_count'})
    hourly_reset_pass_count = hourly_reset_pass_count.to_dict('r')
    json_data = JsonResponse(hourly_reset_pass_count, safe=False)
    return json_data


def jan_weekday_reset_pass_count(request):
    jan_weekday_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['January']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_weekday_reset_pass_count'})
    jan_weekday_reset_pass_count = Sort_Dataframeby_Weekday(df=jan_weekday_reset_pass_count,Weekdaycolumnname='weekday')
    jan_weekday_reset_pass_count = jan_weekday_reset_pass_count.to_dict('r')
    json_data = JsonResponse(jan_weekday_reset_pass_count, safe=False)
    return json_data


def jan_daily_reset_pass_count(request):
    jan_daily_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['January']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_daily_reset_pass_count'})
    jan_daily_reset_pass_count = jan_daily_reset_pass_count.to_dict('r')
    json_data = JsonResponse(jan_daily_reset_pass_count, safe=False)
    return json_data


def jan_hourly_reset_pass_count(request):
    jan_hourly_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['January']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jan_hourly_reset_pass_count'})
    jan_hourly_reset_pass_count = jan_hourly_reset_pass_count.to_dict('r')
    json_data = JsonResponse(jan_hourly_reset_pass_count, safe=False)
    return json_data


def feb_weekday_reset_pass_count(request):
    feb_weekday_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['February']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_weekday_reset_pass_count'})
    feb_weekday_reset_pass_count = Sort_Dataframeby_Weekday(df=feb_weekday_reset_pass_count,Weekdaycolumnname='weekday')
    feb_weekday_reset_pass_count = feb_weekday_reset_pass_count.to_dict('r')
    json_data = JsonResponse(feb_weekday_reset_pass_count, safe=False)
    return json_data


def feb_daily_reset_pass_count(request):
    feb_daily_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['February']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_daily_reset_pass_count'})
    feb_daily_reset_pass_count = feb_daily_reset_pass_count.to_dict('r')
    json_data = JsonResponse(feb_daily_reset_pass_count, safe=False)
    return json_data


def feb_hourly_reset_pass_count(request):
    feb_hourly_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['February']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'feb_hourly_reset_pass_count'})
    feb_hourly_reset_pass_count = feb_hourly_reset_pass_count.to_dict('r')
    json_data = JsonResponse(feb_hourly_reset_pass_count, safe=False)
    return json_data


def mar_weekday_reset_pass_count(request):
    mar_weekday_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['March']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_weekday_reset_pass_count'})
    mar_weekday_reset_pass_count = Sort_Dataframeby_Weekday(df=mar_weekday_reset_pass_count,Weekdaycolumnname='weekday')
    mar_weekday_reset_pass_count = mar_weekday_reset_pass_count.to_dict('r')
    json_data = JsonResponse(mar_weekday_reset_pass_count, safe=False)
    return json_data


def mar_daily_reset_pass_count(request):
    mar_daily_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['March']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_daily_reset_pass_count'})
    mar_daily_reset_pass_count = mar_daily_reset_pass_count.to_dict('r')
    json_data = JsonResponse(mar_daily_reset_pass_count, safe=False)
    return json_data


def mar_hourly_reset_pass_count(request):
    mar_hourly_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['March']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'mar_hourly_reset_pass_count'})
    mar_hourly_reset_pass_count = mar_hourly_reset_pass_count.to_dict('r')
    json_data = JsonResponse(mar_hourly_reset_pass_count, safe=False)
    return json_data


def apr_weekday_reset_pass_count(request):
    apr_weekday_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['April']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_weekday_reset_pass_count'})
    apr_weekday_reset_pass_count = Sort_Dataframeby_Weekday(df=apr_weekday_reset_pass_count,Weekdaycolumnname='weekday')
    apr_weekday_reset_pass_count = apr_weekday_reset_pass_count.to_dict('r')
    json_data = JsonResponse(apr_weekday_reset_pass_count, safe=False)
    return json_data


def apr_daily_reset_pass_count(request):
    apr_daily_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['April']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_daily_reset_pass_count'})
    apr_daily_reset_pass_count = apr_daily_reset_pass_count.to_dict('r')
    json_data = JsonResponse(apr_daily_reset_pass_count, safe=False)
    return json_data


def apr_hourly_reset_pass_count(request):
    apr_hourly_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['April']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'apr_hourly_reset_pass_count'})
    apr_hourly_reset_pass_count = apr_hourly_reset_pass_count.to_dict('r')
    json_data = JsonResponse(apr_hourly_reset_pass_count, safe=False)
    return json_data


def may_weekday_reset_pass_count(request):
    may_weekday_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['May']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_weekday_reset_pass_count'})
    may_weekday_reset_pass_count = Sort_Dataframeby_Weekday(df=may_weekday_reset_pass_count,Weekdaycolumnname='weekday')
    may_weekday_reset_pass_count = may_weekday_reset_pass_count.to_dict('r')
    json_data = JsonResponse(may_weekday_reset_pass_count, safe=False)
    return json_data


def may_daily_reset_pass_count(request):
    may_daily_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['May']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_daily_reset_pass_count'})
    may_daily_reset_pass_count = may_daily_reset_pass_count.to_dict('r')
    json_data = JsonResponse(may_daily_reset_pass_count, safe=False)
    return json_data


def may_hourly_reset_pass_count(request):
    may_hourly_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['May']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'may_hourly_reset_pass_count'})
    may_hourly_reset_pass_count = may_hourly_reset_pass_count.to_dict('r')
    json_data = JsonResponse(may_hourly_reset_pass_count, safe=False)
    return json_data


def jun_weekday_reset_pass_count(request):
    jun_weekday_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['June']))], index='weekday', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_weekday_reset_pass_count'})
    jun_weekday_reset_pass_count = Sort_Dataframeby_Weekday(df=jun_weekday_reset_pass_count,Weekdaycolumnname='weekday')
    jun_weekday_reset_pass_count = jun_weekday_reset_pass_count.to_dict('r')
    json_data = JsonResponse(jun_weekday_reset_pass_count, safe=False)
    return json_data


def jun_daily_reset_pass_count(request):
    jun_daily_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['June']))], index='day', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_daily_reset_pass_count'})
    jun_daily_reset_pass_count = jun_daily_reset_pass_count.to_dict('r')
    json_data = JsonResponse(jun_daily_reset_pass_count, safe=False)
    return json_data


def jun_hourly_reset_pass_count(request):
    jun_hourly_reset_pass_count = pd.pivot_table(
        data=df.loc[(df['name'].isin(['Reset_password']) & df['month'].isin(['June']))], index='hour', values='amount',
        aggfunc='count').reset_index().rename(columns={'amount': 'jun_hourly_reset_pass_count'})
    jun_hourly_reset_pass_count = jun_hourly_reset_pass_count.to_dict('r')
    json_data = JsonResponse(jun_hourly_reset_pass_count, safe=False)
    return json_data
# fig = plt.figure(figsize=(15,8))
# sns.countplot(x='name', data=df)
# plt.title('Distribution of Services')
#
# fig = plt.figure(figsize=(15,8))
# sns.countplot(x='month', data=df);
# plt.title('Distribution of Monthly Transactions');
# plt.show()
# # N = 50
# # x = np.random.rand(N)
# # y = np.random.rand(N)
# # colors = np.random.rand(N)
# # area = np.pi * (15 * np.random.rand(N))**2  # 0 to 15 point radii
# # plt.scatter(x, y, s=area, c=colors, alpha=0.5)
# # plt.show()
# #
# # X = np.linspace(-np.pi, np.pi, 256,endpoint=True)
# # C,S = np.cos(X), np.sin(X)
# #
# # plt.plot(X, C, color="blue", linewidth=2.5, linestyle="-")
# # plt.plot(X, S, color="red", linewidth=2.5, linestyle="-")
# #
# # plt.xlim(X.min()*1.1, X.max()*1.1)
# # plt.xticks([-np.pi, -np.pi/2, 0, np.pi/2, np.pi],
# # [r'$-\pi$', r'$-\pi/2$', r'$0$', r'$+\pi/2$', r'$+\pi$'])
# #
# # plt.ylim(C.min()*1.1,C.max()*1.1)
# # plt.yticks([-1, 0, +1],
# # [r'$-1$', r'$0$', r'$+1$'])
# #
# # plt.show()
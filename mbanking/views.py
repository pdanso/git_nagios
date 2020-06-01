import json
import os

from sqlalchemy import create_engine
import psycopg2
from sort_dataframeby_monthorweek import *
import pandas as pd

from django.http import JsonResponse

engine = create_engine('postgresql+psycopg2://explorelive:explorelive@localhost:5432/mbanking')

df = pd.read_sql_query('''
SELECT cap.account_number, tr.transaction_id, c.gender, s.service_name, tr.amount,tr.status, tr.date_created 
from public.transactions_requests as tr
join public.customers_account_profile as cap
on tr.account_profile_id = cap.account_profile_id
join public.customers as c
on tr.customer_id = c.customer_uuid
join public.services as s
on tr.service_id = s.service_id
''', engine)
pd.options.display.float_format = 'GHS {:,.2f}'.format
df['date_created'] = pd.to_datetime(df['date_created'])
df['date'] = pd.to_datetime(df['date_created']).dt.date
df['year'] = df['date_created'].dt.year
df['monthname'] = df['date_created'].dt.month_name()
df['weekday'] = df['date_created'].dt.day_name()
df['day'] = df['date_created'].dt.day
df['hour'] = df['date_created'].dt.hour
df['quarter'] = df['date_created'].dt.quarter
df['month'] = df['date_created'].dt.month

print(df)

sub_df = pd.read_sql_query('''
select cap.account_number,c.fullname, c.dob, c.gender, c.active, c.approved, cma.branch, c.last_update
from customers as c
join customers_account_profile as cap
on c.customer_uuid = cap.customer_id
join customers_signup_account_verification as csav
on cap.account_number = csav.account_number
join customers_mbanking_applications as cma
on csav.verification_uuid = cma.verification_details_id
''',engine)

sub_df['last_update'] = pd.to_datetime(sub_df['last_update'])
sub_df['date'] = pd.to_datetime(sub_df['last_update']).dt.date
sub_df['year'] = sub_df['last_update'].dt.year
sub_df['monthname'] = sub_df['last_update'].dt.month_name()
sub_df['weekday'] = sub_df['last_update'].dt.day_name()
sub_df['day'] = sub_df['last_update'].dt.day
sub_df['hour'] = sub_df['last_update'].dt.hour
sub_df['quarter'] = sub_df['last_update'].dt.quarter
sub_df['month'] = sub_df['last_update'].dt.month

print(sub_df)

def index(request):
    return JsonResponse('hhhh', safe=False)


def total_amount(request):
    return JsonResponse((df['amount'].sum()), safe=False)


def service(request):
    service = pd.pivot_table(df[df['year'] == 2020], values=['amount', 'transaction_id'], index=['service_name'],
                             aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
        columns={'amount': 'service_revenue', 'transaction_id': 'service_count'})
    service = service.to_dict('r')

    json_data = JsonResponse(service, safe=False)
    return json_data


def status_count(request):
    status_count = df.groupby(['status'])['amount'].count().reset_index().rename(columns={'amount': 'status_count'})
    status_count = status_count.to_dict('r')
    json_data = JsonResponse(status_count, safe=False)
    return json_data


def all_monthly_service(request):
    services = ['BALANCE', 'MOBILE MONEY COLLECTIONS', 'AIRTIME', 'MOBILE MONEY DEPOSITS', 'INTERNAL TRANSFER']
    results = {}
    for service in services:
        #     print(month)
        result = pd.pivot_table(df[(df['service_name'] == service) & (df['year'] == 2020)],
                                values=['amount', 'transaction_id'],
                                index=['service_name', 'monthname'],
                                aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
            columns={'amount': 'service_revenue', 'transaction_id': 'service_count'})
        results[service] = result.to_dict('r')
    json_data = JsonResponse(results, safe=False)
    return json_data


def all_weekday_service(request):
    services = ['BALANCE', 'MOBILE MONEY COLLECTIONS', 'AIRTIME', 'MOBILE MONEY DEPOSITS', 'INTERNAL TRANSFER']
    results = {}
    for service in services:
        #     print(month)
        result = pd.pivot_table(df[(df['service_name'] == service) & (df['year'] == 2020)],
                                values=['amount', 'transaction_id'],
                                index=['service_name', 'weekday'],
                                aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
            columns={'amount': 'service_revenue', 'transaction_id': 'service_count'})
        results[service] = result.to_dict('r')
    json_data = JsonResponse(results, safe=False)
    return json_data


def all_daily_service(request):
    services = ['BALANCE', 'MOBILE MONEY COLLECTIONS', 'AIRTIME', 'MOBILE MONEY DEPOSITS', 'INTERNAL TRANSFER']
    results = {}
    for service in services:
        #     print(month)
        result = pd.pivot_table(df[(df['service_name'] == service) & (df['year'] == 2020)],
                                values=['amount', 'transaction_id'],
                                index=['service_name', 'day'],
                                aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
            columns={'amount': 'service_revenue', 'transaction_id': 'service_count'})
        results[service] = result.to_dict('r')
    json_data = JsonResponse(results, safe=False)
    return json_data


def all_hourly_service(request):
    services = ['BALANCE', 'MOBILE MONEY COLLECTIONS', 'AIRTIME', 'MOBILE MONEY DEPOSITS', 'INTERNAL TRANSFER']
    results = {}
    for service in services:
        #     print(month)
        result = pd.pivot_table(df[(df['service_name'] == service) & (df['year'] == 2020)],
                                values=['amount', 'transaction_id'],
                                index=['service_name', 'hour'],
                                aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
            columns={'amount': 'service_revenue', 'transaction_id': 'service_count'})
        results[service] = result.to_dict('r')
    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_service_trnx(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(df[(df['monthname'] == month) & (df['year'] == 2020)],
                                values=['amount', 'transaction_id'],
                                index=['service_name', 'monthname'],
                                aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
            columns={'amount': month + '_service_revenue', 'transaction_id': month + '_service_count'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data



def monthly_weekday_service_trnx(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(df[(df['monthname'] == month) & (df['year'] == 2020)],
                                values=['amount', 'transaction_id'],
                                index=['service_name', 'weekday'],
                                aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
            columns={'amount': month + '_service_revenue', 'transaction_id': month + '_service_count'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_daily_service_trnx(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(df[(df['monthname'] == month) & (df['year'] == 2020)],
                                values=['amount', 'transaction_id'],
                                index=['service_name', 'day'],
                                aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
            columns={'amount': month + '_service_revenue', 'transaction_id': month + '_service_count'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_hourly_service_trnx(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(df[(df['monthname'] == month) & (df['year'] == 2020)],
                                values=['amount', 'transaction_id'],
                                index=['service_name', 'hour'],
                                aggfunc={'amount': 'sum', 'transaction_id': 'count'}).reset_index().rename(
            columns={'amount': month + '_service_revenue', 'transaction_id': month + '_service_count'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def total_monthly_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['monthname'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def total_monthly_weekday_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['weekday'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def total_monthly_daily_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['day'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def total_monthly_hourly_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['hour'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_gender_based_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=[ 'gender'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_gender_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_weekday_gender_based_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['weekday', 'gender'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_gender_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_daily_gender_based_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['day', 'gender'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_gender_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_hourly_gender_based_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['hour', 'gender'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_gender_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_branch_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=[ 'branch'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_weekday_branch_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['weekday', 'branch'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_daily_branch_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['day', 'branch'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_hourly_branch_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['hour', 'branch'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_active_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=[ 'active'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_weekday_active_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['weekday', 'active'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_daily_active_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['day', 'active'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_hourly_active_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['hour', 'active'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_approved_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=[ 'approved'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_weekday_approved_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['weekday', 'approved'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_daily_approved_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['day', 'approved'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data


def monthly_hourly_approved_subscription(request):
    month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
    results = {}
    for month in month_list:
        #     print(month)
        result = pd.pivot_table(sub_df[(sub_df['monthname'] == month) & (df['year'] == 2020)],
                                values=['account_number'],
                                index=['hour', 'approved'],
                                aggfunc={'account_number': 'count'}).reset_index().rename(
            columns={'account_number': month + '_branch_subscription'})
        results[month] = result.to_dict('r')

    json_data = JsonResponse(results, safe=False)
    return json_data
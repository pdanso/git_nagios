from django.urls import path


from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('total_amount/', views.total_amount, name='total_amount'),
    path('service/', views.service, name='service'),
    path('status_count/', views.status_count, name='status_count'),

    path('all_monthly_service/', views.all_monthly_service, name='all_monthly_service'),
    path('all_weekday_service/', views.all_weekday_service, name='all_weekday_service'),
    path('all_daily_service/', views.all_daily_service, name='all_daily_service'),
    path('all_hourly_service/', views.all_hourly_service, name='all_hourly_service'),

    path('monthly_service_trnx/', views.monthly_service_trnx, name='monthly_service_trnx'),
    path('monthly_weekday_service_trnx/', views.monthly_weekday_service_trnx, name='monthly_weekday_service_trnx'),
    path('monthly_daily_service_trnx/', views.monthly_daily_service_trnx, name='monthly_daily_service_trnx'),
    path('monthly_hourly_service_trnx/', views.monthly_hourly_service_trnx, name='monthly_hourly_service_trnx'),

    path('total_monthly_subscription/', views.total_monthly_subscription, name='total_monthly_subscription'),
    path('total_monthly_weekday_subscription/', views.total_monthly_weekday_subscription, name='total_monthly_weekday_subscription'),
    path('total_monthly_daily_subscription/', views.total_monthly_daily_subscription, name='total_monthly_daily_subscription'),
    path('total_monthly_hourly_subscription/', views.total_monthly_hourly_subscription, name='total_monthly_hourly_subscription'),

    path('monthly_gender_based_subscription/', views.monthly_gender_based_subscription, name='monthly_gender_based_subscription'),
    path('monthly_weekday_gender_based_subscription/', views.monthly_weekday_gender_based_subscription, name='monthly_weekday_gender_based_subscription'),
    path('monthly_daily_gender_based_subscription/', views.monthly_daily_gender_based_subscription, name='monthly_daily_gender_based_subscription'),
    path('monthly_hourly_gender_based_subscription/', views.monthly_hourly_gender_based_subscription, name='monthly_hourly_gender_based_subscription'),

    path('monthly_branch_subscription/', views.monthly_branch_subscription, name='monthly_branch_subscription'),
    path('monthly_weekday_branch_subscription/', views.monthly_weekday_branch_subscription, name='monthly_weekday_branch_subscription'),
    path('monthly_daily_branch_subscription/', views.monthly_daily_branch_subscription, name='monthly_daily_branch_subscription'),
    path('monthly_hourly_branch_subscription/', views.monthly_hourly_branch_subscription, name='monthly_hourly_branch_subscription'),

    path('monthly_active_subscription/', views.monthly_active_subscription, name='monthly_active_subscription'),
    path('monthly_weekday_active_subscription/', views.monthly_weekday_active_subscription, name='monthly_weekday_active_subscription'),
    path('monthly_daily_active_subscription/', views.monthly_daily_active_subscription, name='monthly_daily_active_subscription'),
    path('monthly_hourly_active_subscription/', views.monthly_hourly_active_subscription, name='monthly_hourly_active_subscription'),

    path('monthly_approved_subscription/', views.monthly_approved_subscription, name='monthly_approved_subscription'),
    path('monthly_weekday_approved_subscription/', views.monthly_weekday_approved_subscription, name='monthly_weekday_approved_subscription'),
    path('monthly_daily_approved_subscription/', views.monthly_daily_approved_subscription, name='monthly_daily_approved_subscription'),
    path('monthly_hourly_approved_subscription/', views.monthly_hourly_approved_subscription, name='monthly_hourly_approved_subscription'),

]
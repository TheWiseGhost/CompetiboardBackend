from django.urls import path
from .views import main, create_user, board_options, add_board, update_data_source, data_details, update_data_settings, update_display, board_details, generate_leaderboard
from .views import generate_30_days_leaderboard, create_checkout_session, stripe_webhook, public_board_details, public_generate_leaderboard, public_generate_30_days_leaderboard
from .views import user_details

urlpatterns = [
    path('', main),
    path("clerk/webhook/", create_user),
    path('board_options/', board_options),
    path('board_details/', board_details),
    path('add_board/', add_board),
    path('update_data_source/', update_data_source),
    path('update_data_settings/', update_data_settings),
    path('update_display/', update_display),
    path('data_details/', data_details),
    path('generate_leaderboard/', generate_leaderboard),
    path('generate_30_days_leaderboard/', generate_30_days_leaderboard),
    path("create_checkout_session/", create_checkout_session, name="create_checkout_session"),
    path("stripe/webhook/", stripe_webhook, name="stripe-webhook"),
    path('public_board_details/', public_board_details),
    path('public_generate_leaderboard/', public_generate_leaderboard),
    path('public_generate_30_days_leaderboard/', public_generate_30_days_leaderboard),
    path('user_details/', user_details),
]

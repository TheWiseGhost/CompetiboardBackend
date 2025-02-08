from django.urls import path
from .views import main, create_user, board_options, add_board, update_data_source, data_details, update_data_settings, update_display, board_details, generate_leaderboard

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
]

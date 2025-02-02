from django.urls import path
from .views import main, create_user, board_options, add_board

urlpatterns = [
    path('', main),
    path("clerk/webhook/", create_user),
    path('board_options/', board_options),
    path('add_board/', add_board),
]

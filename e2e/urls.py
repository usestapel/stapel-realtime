"""The emit endpoints the e2e driver pokes."""
from django.urls import path

from . import views

urlpatterns = [
    path("emit", views.emit),
    path("emit-rollback", views.emit_rollback),
    path("health", views.health),
]

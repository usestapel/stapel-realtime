"""A stand-in for a module's row — the thing the signal describes."""
from django.db import models


class Beat(models.Model):
    label = models.CharField(
        max_length=64, help_text="Which emit run wrote this row."
    )
    created_at = models.DateTimeField(
        auto_now_add=True, help_text="When the row was committed."
    )

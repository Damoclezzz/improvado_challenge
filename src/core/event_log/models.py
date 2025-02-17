from django.db import models

from core.models import TimeStampedModel


class OutboxEvent(TimeStampedModel):
    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        COMPLETED = 'completed', 'Completed'
        FAILED = 'failed', 'Failed'

    event_type = models.CharField(max_length=255)
    event_data = models.JSONField()
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    retry_count = models.IntegerField(default=0)
    error_message = models.TextField(null=True)
    processed_at = models.DateTimeField(null=True)


class BatchFailure(TimeStampedModel):
    error_message = models.TextField()
    resolved = models.BooleanField(default=False)
    resolved_at = models.DateTimeField(null=True)
    resolution_comment = models.TextField(null=True)
    events = models.ManyToManyField(
        OutboxEvent,
        related_name='batch_failures',
    )

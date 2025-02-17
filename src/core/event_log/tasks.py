import structlog
from celery import Task, shared_task
from django.conf import settings
from django.db import models, transaction
from django.utils import timezone

from core.base_model import Model
from core.event_log.client import EventLogClient
from core.event_log.exceptions import BatchProcessingError
from core.event_log.models import BatchFailure, OutboxEvent

logger = structlog.get_logger(__name__)

class EventModel(Model):
    event_type: str
    event_data: dict

class ProcessOutboxEventsTask(Task):
    name = 'core.process_outbox_events'
    max_retries = 3
    default_retry_delay = 60

    @transaction.atomic
    def run(self) -> None:
        events = self._get_processable_events()

        if not events:
            logger.debug("no_pending_events_found")
            return

        batches = self._split_into_batches(events)
        successful_events, failed_events = self._process_batches(batches)

        if successful_events:
            self._handle_successful_events(successful_events)

        if failed_events:
            self._handle_failed_events(failed_events)

    def _split_into_batches(self, events: list[OutboxEvent]) -> list[list[OutboxEvent]]:
        return [
            events[i:i + settings.CLICKHOUSE_BATCH_SIZE]
            for i in range(0, len(events), settings.CLICKHOUSE_BATCH_SIZE)
        ]

    def _process_batches(self, batches: list[list[OutboxEvent]]) -> tuple[list[OutboxEvent], list[OutboxEvent]]:
        successful_events = []
        failed_events = []

        with EventLogClient.init() as client:
            for batch in batches:
                try:
                    self._insert_batch(client, batch)
                    successful_events.extend(batch)
                except Exception as e:
                    logger.error(
                        "batch_processing_failed",
                        batch_size=len(batch),
                        error=str(e),
                    )
                    failed_events.extend(batch)
                    self._record_batch_failure(batch, e)

        return successful_events, failed_events

    def _insert_batch(self, client: EventLogClient, batch: list[OutboxEvent]) -> None:
        data = [
            EventModel(event_type=event.event_type, event_data=event.event_data)
            for event in batch
        ]
        client.insert(data)

    def _handle_successful_events(self, successful_events: list[OutboxEvent]) -> None:
        self._mark_events_completed(successful_events)
        logger.info(
            "events_processed_successfully",
            count=len(successful_events),
        )

    def _handle_failed_events(self, failed_events: list[OutboxEvent]) -> None:
        error = BatchProcessingError(f"Failed to process {len(failed_events)} events")
        self._mark_events_failed(failed_events, error)

        logger.error(
            "events_processing_failed",
            count=len(failed_events),
        )

        logger.info(f"Retrying task due to error: {error} {type(error)}")
        transaction.on_commit(lambda: self.retry(exc=error))

    def _get_processable_events(self) -> list[OutboxEvent]:
        return list(
            OutboxEvent.objects
            .filter(
                status__in=[OutboxEvent.Status.PENDING, OutboxEvent.Status.FAILED],
                retry_count__lt=self.max_retries,
            )
            .select_for_update(skip_locked=True),
        )

    def _mark_events_completed(self, events: list[OutboxEvent]) -> None:
        event_ids = [event.id for event in events]
        OutboxEvent.objects.filter(id__in=event_ids).update(
            status=OutboxEvent.Status.COMPLETED,
            processed_at=timezone.now(),
        )

    def _mark_events_failed(self, events: list[OutboxEvent], error: Exception) -> None:
        event_ids = [event.id for event in events]
        updated_count = OutboxEvent.objects.filter(id__in=event_ids).update(
            status=OutboxEvent.Status.FAILED,
            error_message=str(error),
            retry_count=models.F('retry_count') + 1,
        )
        logger.info(f"Updated {updated_count} events to FAILED status.")

    def _record_batch_failure(self, events: list[OutboxEvent], error: Exception) -> None:
        batch_failure = BatchFailure.objects.create(
            error_message=str(error),
        )

        through_model = BatchFailure.events.through
        through_model.objects.bulk_create([
            through_model(
                batchfailure_id=batch_failure.id,
                outboxevent_id=event.id,
            ) for event in events
        ])

        logger.error(
            "batch_failure_recorded",
            batch_failure_id=batch_failure.id,
            events_count=len(events),
            error=str(error),
        )

process_outbox_events = shared_task(
    base=ProcessOutboxEventsTask,
    bind=True,
)

from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from celery.exceptions import Retry

from core.event_log.exceptions import BatchProcessingError
from core.event_log.models import BatchFailure, OutboxEvent
from core.event_log.tasks import ProcessOutboxEventsTask

pytestmark = [pytest.mark.django_db]


@pytest.fixture
def task() -> ProcessOutboxEventsTask:
    def retry_side_effect(exc: Exception) -> Retry:
        raise Retry(exc=exc)

    task_instance: ProcessOutboxEventsTask = ProcessOutboxEventsTask()
    task_instance.retry = MagicMock(side_effect=retry_side_effect)
    return task_instance


@pytest.fixture
def pending_event() -> OutboxEvent:
    return OutboxEvent.objects.create(
        event_type='TestEvent',
        event_data={'test': 'data'},
        status=OutboxEvent.Status.PENDING,
    )


@pytest.fixture
def failed_event() -> OutboxEvent:
    return OutboxEvent.objects.create(
        event_type='TestEvent',
        event_data={'test': 'data'},
        status=OutboxEvent.Status.FAILED,
    )


@pytest.fixture
def mock_on_commit() -> Generator[list[Callable[[], Any]]]:
    with patch('django.db.transaction.on_commit') as mock:
        callbacks: list[Callable[[], Any]] = []

        def collect_callback(callback: Callable[[], Any]) -> None:
            callbacks.append(callback)

        mock.side_effect = collect_callback
        yield callbacks


def test_retry_on_error(
    task: ProcessOutboxEventsTask,
    pending_event: OutboxEvent,
    mock_on_commit: list[Callable[[], Any]],
) -> None:
    with patch('core.event_log.tasks.EventLogClient') as mock_client:
        mock_client.init.return_value.__enter__.return_value.insert.side_effect = Exception("Test error")

        pending_event.retry_count = 1
        pending_event.save()

        task.run()

        with pytest.raises(Retry) as exc_info:
            for callback in mock_on_commit:
                callback()

        assert isinstance(exc_info.value.args[0].exc, BatchProcessingError)

        pending_event.refresh_from_db()
        assert task.retry.called
        assert pending_event.status == OutboxEvent.Status.FAILED
        assert pending_event.retry_count == 2

        assert BatchFailure.objects.count() == 1
        batch_failure: BatchFailure = BatchFailure.objects.get()
        assert batch_failure.error_message == "Test error"
        assert list(batch_failure.events.all()) == [pending_event]


def test_no_retry_when_max_retries_exceeded(
    task: ProcessOutboxEventsTask,
    failed_event: OutboxEvent,
    mock_on_commit: list[Callable[[], Any]],
) -> None:
    with patch('core.event_log.tasks.EventLogClient') as mock_client:
        mock_client.init.return_value.__enter__.return_value.insert.side_effect = Exception("Test error")

        failed_event.retry_count = task.max_retries
        failed_event.save()

        task.run()

        for callback in mock_on_commit:
            callback()

        assert not task.retry.called

        failed_event.refresh_from_db()
        assert failed_event.status == OutboxEvent.Status.FAILED
        assert failed_event.retry_count == task.max_retries

        assert BatchFailure.objects.count() == 0


def test_successful_processing(
    task: ProcessOutboxEventsTask,
    pending_event: OutboxEvent,
) -> None:
    with patch('core.event_log.tasks.EventLogClient'):
        task.run()

        pending_event.refresh_from_db()
        assert pending_event.status == OutboxEvent.Status.COMPLETED
        assert pending_event.processed_at is not None
        assert BatchFailure.objects.count() == 0
        assert not task.retry.called


def test_failed_processing_creates_batch_failure(
    task: ProcessOutboxEventsTask,
    pending_event: OutboxEvent,
    mock_on_commit: list[Callable[[], Any]],
) -> None:
    with patch('core.event_log.tasks.EventLogClient') as mock_client:
        mock_client.init.return_value.__enter__.return_value.insert.side_effect = Exception("Test error")

        task.run()

        with pytest.raises(Retry) as exc_info:
            for callback in mock_on_commit:
                callback()

        assert isinstance(exc_info.value.args[0].exc, BatchProcessingError)

        pending_event.refresh_from_db()
        assert task.retry.called
        assert pending_event.status == OutboxEvent.Status.FAILED
        assert pending_event.retry_count == 1

        assert BatchFailure.objects.count() == 1
        batch_failure: BatchFailure = BatchFailure.objects.get()
        assert batch_failure.error_message == "Test error"
        assert list(batch_failure.events.all()) == [pending_event]

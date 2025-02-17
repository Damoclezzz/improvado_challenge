import pytest

from core.event_log.models import OutboxEvent
from core.event_log.services import EventService

pytestmark = [pytest.mark.django_db]


def test_publish_event() -> None:
    event = EventService.publish_event(
        event_type='TestEvent',
        event_data={'test': 'data'},
    )

    assert isinstance(event, OutboxEvent)
    assert event.event_type == 'TestEvent'
    assert event.event_data == {'test': 'data'}
    assert event.status == OutboxEvent.Status.PENDING
    assert event.retry_count == 0

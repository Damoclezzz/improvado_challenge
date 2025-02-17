import structlog

from core.event_log.models import OutboxEvent

logger = structlog.get_logger(__name__)


class EventService:
    @classmethod
    def publish_event(cls, event_type: str, event_data: dict) -> OutboxEvent:
        logger.info("publishing_event", event_type=event_type)

        return OutboxEvent.objects.create(
            event_type=event_type,
            event_data=event_data,
        )

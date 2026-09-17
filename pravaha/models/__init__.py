"""SQLAlchemy ORM models for Pravaha.

Grouped by concern but all attached to the single :class:`pravaha.db.Base`
metadata so Alembic autogenerate sees everything.
"""

from pravaha.models.ai import (
    AIEvidence,
    AIHypothesis,
    AIInvestigation,
    AIToolCall,
    AIUsage,
)
from pravaha.models.alerting import Alert, AlertRule
from pravaha.models.audit import AuditLog
from pravaha.models.batch import BatchEventRollup
from pravaha.models.consumers import ConsumerGroup, ConsumerInstance, ConsumerLag
from pravaha.models.dataquality import DataQualityRecord
from pravaha.models.dlq import DeadLetterEvent
from pravaha.models.events import Event, EventProcessingRecord
from pravaha.models.pipelines import (
    Pipeline,
    PipelineEdge,
    PipelineExecution,
    PipelineNode,
    PipelineVersion,
)
from pravaha.models.processing import Aggregation, AnomalyRecord, WindowRecord
from pravaha.models.producers import Producer
from pravaha.models.replay import ReplayJob
from pravaha.models.schemas import EventSchema, SchemaVersion
from pravaha.models.users import Role, User

__all__ = [
    "AIEvidence",
    "AIHypothesis",
    "AIInvestigation",
    "AIToolCall",
    "AIUsage",
    "Aggregation",
    "Alert",
    "AlertRule",
    "AnomalyRecord",
    "AuditLog",
    "BatchEventRollup",
    "ConsumerGroup",
    "ConsumerInstance",
    "ConsumerLag",
    "DataQualityRecord",
    "DeadLetterEvent",
    "Event",
    "EventProcessingRecord",
    "EventSchema",
    "Pipeline",
    "PipelineEdge",
    "PipelineExecution",
    "PipelineNode",
    "PipelineVersion",
    "Producer",
    "ReplayJob",
    "Role",
    "SchemaVersion",
    "User",
    "WindowRecord",
]

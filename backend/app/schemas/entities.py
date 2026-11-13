from pydantic import BaseModel


class ApplyRequest(BaseModel):
    stream_id: int


class StreamPatch(BaseModel):
    enabled: bool | None = None


class SettingPut(BaseModel):
    value: str


class RetentionPut(BaseModel):
    retention_days: int


class CleanupPreviewRequest(BaseModel):
    retention_days: int | None = None


class LegalHoldCreate(BaseModel):
    scope: str
    stream_id: int | None = None
    entity_key: str | None = None
    start_ts: str | None = None
    end_ts: str | None = None
    reason: str

from pydantic import BaseModel, Field
from typing import Optional, Literal

class NotificationPrefsIn(BaseModel):
    productNews: Optional[bool] = None
    announcements: Optional[bool] = None
    offers: Optional[bool] = None
    securityOnly: Optional[bool] = None
    email: Optional[bool] = None
    inapp: Optional[bool] = None
    sms: Optional[bool] = None

class NotificationPrefsOut(NotificationPrefsIn):
    user_id: str

class PrivacyPrefsIn(BaseModel):
    analyticsConsent: Optional[bool] = None
    essentialOnly: Optional[bool] = None
    studentTargeting: Optional[bool] = None
    shareForResearch: Optional[bool] = None

class PrivacyPrefsOut(PrivacyPrefsIn):
    user_id: str
    lastConsentAt: Optional[str] = None  # ISO 8601

ExportJobStatus = Literal["PENDING", "PROCESSING", "READY", "FAILED", "EXPIRED"]

class DataExportJobOut(BaseModel):
    id: str
    user_id: str
    status: ExportJobStatus
    bucket: Optional[str] = None
    objectKey: Optional[str] = None
    readyAt: Optional[str] = None
    expiresAt: Optional[str] = None
    createdAt: str
    updatedAt: str

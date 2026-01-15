from pydantic import BaseModel, Field
from typing import Literal


class PurchaseCreatedEvent(BaseModel):
    eventId: str
    eventType: Literal["PurchaseCreated"] = "PurchaseCreated"
    eventVersion: int = 1
    timestamp: str

    userId: str
    itemId: str
    quantity: int = Field(ge=1)

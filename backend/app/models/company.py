from sqlalchemy import Column, String, JSON
from sqlalchemy.dialects.postgresql import UUID
import uuid
from ..core.db import Base

class Company(Base):
    __tablename__ = "companies"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    domain = Column(String, unique=True, index=True, nullable=True)
    identifiers = Column(JSON, nullable=True)   # {'companies_house': '...', ...}
    profile_data = Column(JSON, nullable=True)  # consolidated firmographics


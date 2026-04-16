import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, Text, DateTime, Boolean, Integer, ForeignKey, Index
from sqlalchemy.orm import relationship

from database import Base


def new_uuid() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Listing(Base):
    __tablename__ = "listings"

    id = Column(Text, primary_key=True, default=new_uuid)
    adid = Column(Text, unique=True, nullable=False, index=True)
    first_seen_at = Column(DateTime, default=utcnow)
    last_seen_at = Column(DateTime, default=utcnow, onupdate=utcnow)
    current_version_id = Column(Text, ForeignKey("listing_versions.id"), nullable=True)

    versions = relationship("ListingVersion", back_populates="listing", foreign_keys="ListingVersion.listing_id")
    current_version = relationship("ListingVersion", foreign_keys=[current_version_id], post_update=True)
    images = relationship("Image", back_populates="listing")


class ListingVersion(Base):
    __tablename__ = "listing_versions"

    id = Column(Text, primary_key=True, default=new_uuid)
    listing_id = Column(Text, ForeignKey("listings.id"), nullable=False, index=True)
    fetched_at = Column(DateTime, default=utcnow)

    title = Column(Text)
    description = Column(Text)
    url = Column(Text)
    status = Column(Text)

    price_amount = Column(Text)
    price_currency = Column(Text)
    price_negotiable = Column(Boolean)

    location_zip = Column(Text)
    location_city = Column(Text)
    location_state = Column(Text)

    delivery = Column(Text)
    views = Column(Text)

    categories = Column(Text)  # JSON
    details = Column(Text)     # JSON
    features = Column(Text)    # JSON
    seller = Column(Text)      # JSON
    extra_info = Column(Text)  # JSON
    image_urls = Column(Text)  # JSON

    data_hash = Column(Text, index=True)

    listing = relationship("Listing", back_populates="versions", foreign_keys=[listing_id])
    images = relationship("Image", back_populates="version")


class Image(Base):
    __tablename__ = "images"

    id = Column(Text, primary_key=True, default=new_uuid)
    listing_id = Column(Text, ForeignKey("listings.id"), nullable=False, index=True)
    version_id = Column(Text, ForeignKey("listing_versions.id"), nullable=False)
    original_url = Column(Text, nullable=False)
    local_path = Column(Text)
    downloaded_at = Column(DateTime)
    file_size = Column(Integer)
    content_type = Column(Text)
    status = Column(Text, default="pending")  # pending / downloaded / failed

    listing = relationship("Listing", back_populates="images")
    version = relationship("ListingVersion", back_populates="images")

    __table_args__ = (
        Index("ix_images_listing_url", "listing_id", "original_url"),
    )

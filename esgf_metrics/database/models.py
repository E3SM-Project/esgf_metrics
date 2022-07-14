from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base: declarative_base = declarative_base()


class LogFile(Base):
    # df_logs
    __tablename__ = "log_file"
    id = Column(Integer, primary_key=True)

    path = Column(String, nullable=False)

    # Path parsed
    node = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    year_month = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    fiscal_year_quarter = Column(String, nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    fiscal_month = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer, nullable=False)

    requests = relationship("LogRequest", back_populates="log_file")
    __table_args__ = (UniqueConstraint("path", name="_path_uc"),)


class LogRequest(Base):
    # df_requests
    __tablename__ = "log_request"
    id = Column(Integer, primary_key=True)
    log_id = Column(Integer, ForeignKey(LogFile.id, ondelete="CASCADE"), nullable=False)

    log_line = Column(String, nullable=False)
    ip_address = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    year_month = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    access_type = Column(String, nullable=False)
    status_code = Column(String, nullable=False)
    dataset_path = Column(String, nullable=False)
    bytes = Column(BigInteger, nullable=False)
    megabytes = Column(Float, nullable=False)
    dataset_id = Column(String, nullable=False)
    file_id = Column(String, nullable=False)
    project = Column(String, nullable=False)
    realm = Column(String, nullable=True)
    data_type = Column(String, nullable=True)
    variable_id = Column(String, nullable=True)
    time_frequency = Column(String, nullable=True)
    activity = Column(String, nullable=True)

    log_file = relationship("LogFile", back_populates="requests")

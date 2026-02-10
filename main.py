import os
from datetime import datetime
from typing import List, Optional
from enum import IntEnum

from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, Session, declarative_base

# --- 1. CONFIGURATION & DATABASE ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fleet.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 2. ENUMS ---
class MaintenanceStatus(IntEnum):
    IN_PROGRESS = 1
    WAITING_FOR_PARTS = 2
    COMPLETED = 3

class MaintenanceType(IntEnum):
    STANDARD_SERVICE = 1
    INITIAL_INSPECTION = 2
    REPAIR = 3

# --- 3. DATABASE MODELS ---
class VehicleModel(Base):
    __tablename__ = "fm_vehicles"
    id = Column(Integer, primary_key=True, index=True)
    make = Column(String)
    model = Column(String)
    year = Column(Integer)
    is_active = Column(Boolean, default=True)
    is_deleted = Column(Boolean, default=False)
    last_service_date = Column(DateTime, nullable=True)

class MaintenanceModel(Base):
    __tablename__ = "fm_maintenances"
    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(Integer, ForeignKey("fm_vehicles.id"))
    technician = Column(String)
    maintenance_type_id = Column(Integer)
    status_id = Column(Integer, default=MaintenanceStatus.IN_PROGRESS)
    created_on = Column(DateTime, default=datetime.utcnow)
    completed_on = Column(DateTime, nullable=True)

    vehicle = relationship("VehicleModel")
    part_orders = relationship("PartOrderModel", back_populates="maintenance")

class PartOrderModel(Base):
    __tablename__ = "fm_part_orders"
    id = Column(Integer, primary_key=True, index=True)
    maintenance_id = Column(Integer, ForeignKey("fm_maintenances.id"))
    purchase_card_num = Column(String)
    total_amount = Column(Float)
    purchased_on = Column(DateTime, default=datetime.utcnow)
    installed_on = Column(DateTime, nullable=True)

    maintenance = relationship("MaintenanceModel", back_populates="part_orders")

Base.metadata.create_all(bind=engine)

# --- 4. DTOs ---
class VehicleDTO(BaseModel):
    id: int
    make: str
    model: str
    year: int
    isActive: bool = Field(..., alias="is_active")
    isDeleted: bool = Field(..., alias="is_deleted")
    lastServiceDate: Optional[datetime] = Field(None, alias="last_service_date")
    class Config:
        orm_mode = True
        allow_population_by_field_name = True

class MaintenanceDTO(BaseModel):
    id: int
    vehicleId: int = Field(..., alias="vehicle_id")
    technician: str
    maintenanceTypeId: int = Field(..., alias="maintenance_type_id")
    statusId: int = Field(..., alias="status_id")
    createdOn: datetime = Field(..., alias="created_on")
    completedOn: Optional[datetime] = Field(None, alias="completed_on")
    class Config:
        orm_mode = True
        allow_population_by_field_name = True

class PartOrderDTO(BaseModel):
    id: int
    maintenanceId: int = Field(..., alias="maintenance_id")
    purchaseCardNum: str = Field(..., alias="purchase_card_num")
    totalAmount: float = Field(..., alias="total_amount")
    purchasedOn: datetime = Field(..., alias="purchased_on")
    installedOn: Optional[datetime] = Field(None, alias="installed_on")
    class Config:
        orm_mode = True
        allow_population_by_field_name = True

class CreateVehicleRequest(BaseModel):
    make: str
    model: str
    year: int

class StartMaintenanceRequest(BaseModel):
    vehicleId: int
    technician: str
    maintenanceTypeId: int

class OrderPartsRequest(BaseModel):
    maintenanceId: int
    purchaseCardNum: str
    totalAmount: float

# --- 5. API ENDPOINTS ---
app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- OPTION A: UPDATED GET ENDPOINTS (With 'ids' filtering) ---

@app.get("/vehicles/", response_model=List[VehicleDTO])
def get_vehicles(
    startIndex: int = 0, 
    batchSize: int = 100, 
    ids: Optional[str] = Query(None), # Captures "1,2,3" from Appian
    db: Session = Depends(get_db)
):
    query = db.query(VehicleModel)
    if ids:
        try:
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(VehicleModel.id.in_(id_list))
        except ValueError:
            pass 
    return query.offset(startIndex).limit(batchSize).all()

@app.get("/maintenance/", response_model=List[MaintenanceDTO])
def get_maintenance(
    startIndex: int = 0, 
    batchSize: int = 100, 
    ids: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(MaintenanceModel)
    if ids:
        try:
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(MaintenanceModel.id.in_(id_list))
        except ValueError:
            pass
    return query.offset(startIndex).limit(batchSize).all()

@app.get("/part-orders/", response_model=List[PartOrderDTO])
def get_part_orders(
    startIndex: int = 0, 
    batchSize: int = 100, 
    ids: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(PartOrderModel)
    if ids:
        try:
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(PartOrderModel.id.in_(id_list))
        except ValueError:
            pass
    return query.offset(startIndex).limit(batchSize).all()


# --- WRITE APIs (Unchanged) ---

@app.post("/vehicles/", response_model=VehicleDTO)
def create_vehicle(vehicle: CreateVehicleRequest, db: Session = Depends(get_db)):
    new_vehicle = VehicleModel(
        make=vehicle.make,
        model=vehicle.model,
        year=vehicle.year,
        is_active=True,
        is_deleted=False
    )
    db.add(new_vehicle)
    db.commit()
    db.refresh(new_vehicle)
    return new_vehicle

@app.put("/vehicles/{vehicle_id}/retire", response_model=VehicleDTO)
def retire_vehicle(vehicle_id: int, db: Session = Depends(get_db)):
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == vehicle_id).first()
    if not vehicle:
        raise HTTPException(404, "Vehicle not found")
    vehicle.is_deleted = True
    vehicle.is_active = False 
    db.commit()
    db.refresh(vehicle)
    return vehicle

@app.post("/maintenance/start", response_model=MaintenanceDTO)
def start_maintenance(req: StartMaintenanceRequest, db: Session = Depends(get_db)):
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == req.vehicleId).first()
    if not vehicle:
        raise HTTPException(404, "Vehicle not found")
    if not vehicle.is_active:
        raise HTTPException(400, "Cannot service an inactive or retired vehicle.")

    new_maint = MaintenanceModel(
        vehicle_id=req.vehicleId,
        technician=req.technician,
        maintenance_type_id=req.maintenanceTypeId,
        status_id=MaintenanceStatus.IN_PROGRESS, 
        created_on=datetime.utcnow()
    )
    db.add(new_maint)
    vehicle.is_active = False # Lock Vehicle
    db.commit()
    db.refresh(new_maint)
    return new_maint

@app.post("/maintenance/parts", response_model=PartOrderDTO)
def order_parts(req: OrderPartsRequest, db: Session = Depends(get_db)):
    maint = db.query(MaintenanceModel).filter(MaintenanceModel.id == req.maintenanceId).first()
    if not maint:
        raise HTTPException(404, "Maintenance record

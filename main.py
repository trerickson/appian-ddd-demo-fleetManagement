import os
from datetime import datetime, date
from typing import List, Optional
from enum import IntEnum

from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Date, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, Session, declarative_base

# --- 1. CONFIGURATION & DATABASE ---
# Railway provides DATABASE_URL. We ensure it starts with postgresql://
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fleet.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 2. ENUMS (The "Secret Handshake" with Appian) ---
# These IDs must match the rows in your Appian Reference Tables!
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
    is_active = Column(Boolean, default=True)      # Is it in the fleet?
    is_deleted = Column(Boolean, default=False)    # Is it retired?
    last_service_date = Column(DateTime, nullable=True)

class MaintenanceModel(Base):
    __tablename__ = "fm_maintenances"
    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(Integer, ForeignKey("fm_vehicles.id"))
    technician = Column(String)
    # These store just the ID. Appian joins the text.
    maintenance_type_id = Column(Integer) 
    status_id = Column(Integer, default=MaintenanceStatus.IN_PROGRESS) 
    created_on = Column(DateTime, default=datetime.utcnow)
    completed_on = Column(DateTime, nullable=True)

    # Relationships
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

# Create Tables
Base.metadata.create_all(bind=engine)

# --- 4. DTOs (API Inputs/Outputs) ---

# --- Base DTOs ---
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

# --- Action Request DTOs (The Inputs for your Commands) ---
class CreateVehicleRequest(BaseModel):
    make: str
    model: str
    year: int

class StartMaintenanceRequest(BaseModel):
    vehicleId: int
    technician: str
    maintenanceTypeId: int # e.g., 1 (Standard), 2 (Inspection)

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

# --- GET APIs (Batching for Appian Sync) ---

@app.get("/vehicles/", response_model=List[VehicleDTO])
def get_vehicles(startIndex: int = 0, batchSize: int = 100, db: Session = Depends(get_db)):
    return db.query(VehicleModel).offset(startIndex).limit(batchSize).all()

@app.get("/maintenance/", response_model=List[MaintenanceDTO])
def get_maintenance(startIndex: int = 0, batchSize: int = 100, db: Session = Depends(get_db)):
    return db.query(MaintenanceModel).offset(startIndex).limit(batchSize).all()

@app.get("/part-orders/", response_model=List[PartOrderDTO])
def get_part_orders(startIndex: int = 0, batchSize: int = 100, db: Session = Depends(get_db)):
    return db.query(PartOrderModel).offset(startIndex).limit(batchSize).all()


# --- ACTION APIs (The Domain Logic) ---

# 1. Add New Vehicle
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

# 2. Retire Vehicle (Soft Delete)
@app.put("/vehicles/{vehicle_id}/retire", response_model=VehicleDTO)
def retire_vehicle(vehicle_id: int, db: Session = Depends(get_db)):
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == vehicle_id).first()
    if not vehicle:
        raise HTTPException(404, "Vehicle not found")
    
    vehicle.is_deleted = True
    vehicle.is_active = False # Also remove from active fleet
    db.commit()
    db.refresh(vehicle)
    return vehicle

# 3. Start Maintenance (Transaction: Create Maint + Lock Vehicle)
@app.post("/maintenance/start", response_model=MaintenanceDTO)
def start_maintenance(req: StartMaintenanceRequest, db: Session = Depends(get_db)):
    # A. Validation
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == req.vehicleId).first()
    if not vehicle:
        raise HTTPException(404, "Vehicle not found")
    if not vehicle.is_active:
        raise HTTPException(400, "Cannot service an inactive or retired vehicle.")

    # B. Transaction
    # 1. Create Maintenance Record
    new_maint = MaintenanceModel(
        vehicle_id=req.vehicleId,
        technician=req.technician,
        maintenance_type_id=req.maintenanceTypeId,
        status_id=MaintenanceStatus.IN_PROGRESS, # AUTO-SET STATUS
        created_on=datetime.utcnow()
    )
    db.add(new_maint)
    
    # 2. Lock the Vehicle (Pull from Fleet)
    vehicle.is_active = False
    
    db.commit()
    db.refresh(new_maint)
    return new_maint

# 4. Order Parts (Transaction: Create Order + Update Status)
@app.post("/maintenance/parts", response_model=PartOrderDTO)
def order_parts(req: OrderPartsRequest, db: Session = Depends(get_db)):
    # A. Validation
    maint = db.query(MaintenanceModel).filter(MaintenanceModel.id == req.maintenanceId).first()
    if not maint:
        raise HTTPException(404, "Maintenance record not found")
    
    # B. Transaction
    # 1. Create Part Order
    new_order = PartOrderModel(
        maintenance_id=req.maintenanceId,
        purchase_card_num=req.purchaseCardNum,
        total_amount=req.totalAmount,
        purchased_on=datetime.utcnow()
    )
    db.add(new_order)
    
    # 2. Update Maintenance Status AUTOMATICALLY
    if maint.status_id != MaintenanceStatus.COMPLETED:
        maint.status_id = MaintenanceStatus.WAITING_FOR_PARTS
        
    db.commit()
    db.refresh(new_order)
    return new_order

# 5. Complete Maintenance (Transaction: Close Maint + Return Vehicle)
@app.put("/maintenance/{maintenance_id}/complete", response_model=MaintenanceDTO)
def complete_maintenance(maintenance_id: int, db: Session = Depends(get_db)):
    # A. Fetch
    maint = db.query(MaintenanceModel).filter(MaintenanceModel.id == maintenance_id).first()
    if not maint:
        raise HTTPException(404, "Maintenance record not found")
    
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == maint.vehicle_id).first()

    # B. Transaction
    # 1. Close Maintenance
    maint.status_id = MaintenanceStatus.COMPLETED
    maint.completed_on = datetime.utcnow()
    
    # 2. Return Vehicle to Fleet
    if vehicle:
        vehicle.is_active = True
        vehicle.last_service_date = datetime.utcnow()
        
    db.commit()
    db.refresh(maint)
    return maint

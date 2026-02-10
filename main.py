import os
import requests
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

# --- APPIAN WEBHOOK CONFIGURATION ---
# You will need to create these additional Web APIs in Appian to sync other records!
APPIAN_BASE_URL = "https://YOUR-SITE.appiancloud.com/suite/webapi"
APPIAN_API_KEY = "YOUR_COPIED_API_KEY"

# Specific Endpoints
URL_SYNC_VEHICLE = f"{APPIAN_BASE_URL}/sync-vehicle"
URL_SYNC_MAINTENANCE = f"{APPIAN_BASE_URL}/sync-maintenance"   # Create this in Appian
URL_SYNC_PARTS = f"{APPIAN_BASE_URL}/sync-part-order"         # Create this in Appian

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

# --- HELPER: GENERIC WEBHOOK TRIGGER ---
def trigger_appian_sync(url: str, record_id: int):
    """Helper to fire-and-forget the Appian Sync Webhook"""
    if "YOUR-SITE" in url:
        return # Skip if not configured

    try:
        print(f"Triggering Appian Sync at {url} for ID {record_id}...")
        requests.post(
            url,
            json={"id": record_id},
            headers={"Appian-API-Key": APPIAN_API_KEY},
            timeout=2
        )
    except Exception as e:
        print(f"Failed to trigger Appian sync: {e}")

# --- 5. API ENDPOINTS ---
app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- READ ENDPOINTS (With "Targeted Get" Support) ---

@app.get("/vehicles/", response_model=List[VehicleDTO])
def get_vehicles(
    startIndex: int = 0, 
    batchSize: int = 100, 
    ids: Optional[str] = Query(None), # Appian passes "1,2,3" here
    db: Session = Depends(get_db)
):
    query = db.query(VehicleModel)
    if ids:
        try:
            # "Targeted Get" Logic: Filter by ID list
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(VehicleModel.id.in_(id_list))
        except ValueError:
            pass 
    return query.offset(startIndex).limit(batchSize).all()

@app.get("/maintenance/", response_model=List[MaintenanceDTO])
def get_maintenance(
    startIndex: int = 0, 
    batchSize: int = 100, 
    ids: Optional[str] = Query(None), # Added for Targeted Sync
    db: Session = Depends(get_db)
):
    query = db.query(MaintenanceModel)
    if ids:
        try:
            # "Targeted Get" Logic
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(MaintenanceModel.id.in_(id_list))
        except ValueError:
            pass
    return query.offset(startIndex).limit(batchSize).all()

@app.get("/part-orders/", response_model=List[PartOrderDTO])
def get_part_orders(
    startIndex: int = 0, 
    batchSize: int = 100, 
    ids: Optional[str] = Query(None), # Added for Targeted Sync
    db: Session = Depends(get_db)
):
    query = db.query(PartOrderModel)
    if ids:
        try:
            # "Targeted Get" Logic
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(PartOrderModel.id.in_(id_list))
        except ValueError:
            pass
    return query.offset(startIndex).limit(batchSize).all()


# --- WRITE ENDPOINTS (With Webhook Triggers) ---

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
    
    # Trigger Sync
    trigger_appian_sync(URL_SYNC_VEHICLE, new_vehicle.id)

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
    
    # Trigger Sync (Vehicle changed)
    trigger_appian_sync(URL_SYNC_VEHICLE, vehicle.id)

    return vehicle

@app.post("/maintenance/start", response_model=MaintenanceDTO)
def start_maintenance(req: StartMaintenanceRequest, db: Session = Depends(get_db)):
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == req.vehicleId).first()
    if not vehicle:
        raise HTTPException(404, "Vehicle not found")
    
    new_maint = MaintenanceModel(
        vehicle_id=req.vehicleId,
        technician=req.technician,
        maintenance_type_id=req.maintenanceTypeId,
        status_id=MaintenanceStatus.IN_PROGRESS, 
        created_on=datetime.utcnow()
    )
    db.add(new_maint)
    vehicle.is_active = False 
    db.commit()
    db.refresh(new_maint)

    # Trigger Sync (Maintenance Created)
    trigger_appian_sync(URL_SYNC_MAINTENANCE, new_maint.id)
    # Also Sync Vehicle (Status changed to Inactive)
    trigger_appian_sync(URL_SYNC_VEHICLE, vehicle.id)

    return new_maint

@app.post("/maintenance/parts", response_model=PartOrderDTO)
def order_parts(req: OrderPartsRequest, db: Session = Depends(get_db)):
    maint = db.query(MaintenanceModel).filter(MaintenanceModel.id == req.maintenanceId).first()
    if not maint:
        raise HTTPException(404, "Maintenance record not found")
    
    new_order = PartOrderModel(
        maintenance_id=req.maintenanceId,
        purchase_card_num=req.purchaseCardNum,
        total_amount=req.totalAmount,
        purchased_on=datetime.utcnow()
    )
    db.add(new_order)
    
    if maint.status_id != MaintenanceStatus.COMPLETED:
        maint.status_id = MaintenanceStatus.WAITING_FOR_PARTS
        
    db.commit()
    db.refresh(new_order)

    # Trigger Sync (Part Order Created)
    trigger_appian_sync(URL_SYNC_PARTS, new_order.id)
    # Also Sync Maintenance (Status changed)
    trigger_appian_sync(URL_SYNC_MAINTENANCE, maint.id)

    return new_order

@app.put("/maintenance/{maintenance_id}/complete", response_model=MaintenanceDTO)
def complete_maintenance(maintenance_id: int, db: Session = Depends(get_db)):
    maint = db.query(MaintenanceModel).filter(MaintenanceModel.id == maintenance_id).first()
    if not maint:
        raise HTTPException(404, "Maintenance record not found")
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == maint.vehicle_id).first()

    maint.status_id = MaintenanceStatus.COMPLETED
    maint.completed_on = datetime.utcnow()
    
    if vehicle:
        vehicle.is_active = True
        vehicle.last_service_date = datetime.utcnow()
        
    db.commit()
    db.refresh(maint)

    # Trigger Sync (Maintenance Completed)
    trigger_appian_sync(URL_SYNC_MAINTENANCE, maint.id)
    # Also Sync Vehicle (Active Status & Service Date changed)
    trigger_appian_sync(URL_SYNC_VEHICLE, vehicle.id)

    return maint

import os
import sys
import random
import string
import requests
from datetime import datetime, timedelta  # <--- FIXED: Added missing import
from typing import List, Optional
from enum import IntEnum

from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, ForeignKey, func, text
from sqlalchemy.orm import sessionmaker, relationship, Session, declarative_base, joinedload

# --- 1. CONFIGURATION & CONNECTION TEST ---
# We keep this strict logic because we know it works now.
raw_url = os.getenv("DATABASE_URL")

print("----------------------------------------------------------------")
print(f"DEBUG: Checking for DATABASE_URL...")

if not raw_url:
    print("FATAL ERROR: DATABASE_URL environment variable is MISSING.")
    sys.exit(1)

# Fix for Railway/Heroku "postgres://" vs "postgresql://"
if raw_url.startswith("postgres://"):
    raw_url = raw_url.replace("postgres://", "postgresql://", 1)

try:
    engine = create_engine(raw_url)
    # Test connection immediately
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print("SUCCESS: Connection to Postgres established!")
except Exception as e:
    print(f"FATAL CONNECTION ERROR: {e}")
    sys.exit(1)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- APPIAN WEBHOOK CONFIG ---
APPIAN_SYNC_URL = "https://cs-fed-accelerate.appiancloud.com/suite/webapi/sync-records"
APPIAN_API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIzZDlkMzRjZi1jZDZhLTA2MjAtNDc0ZS00Nzc1M2FhMmI4Y2MifQ.vqMn7rNxpsd0KLDCKx8lbDTmIs_pZ5E7dISXsIsmD3s"

# --- 2. MODELS ---
class MaintenanceStatus(IntEnum):
    IN_PROGRESS = 1
    WAITING_FOR_PARTS = 2
    COMPLETED = 3

class VehicleModel(Base):
    __tablename__ = "fm_vehicles"
    id = Column(Integer, primary_key=True, index=True)
    vin = Column(String, unique=True, index=True) 
    color = Column(String)                        
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
    notes_open = Column(String, nullable=True)   
    notes_close = Column(String, nullable=True)  
    created_on = Column(DateTime, default=datetime.utcnow) # This caused the error before
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

# --- 3. DATABASE RESET & SEED ---
print("DEBUG: Dropping old tables to ensure schema match...")
Base.metadata.drop_all(bind=engine)
print("DEBUG: Creating new tables...")
Base.metadata.create_all(bind=engine)

def generate_vin():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=17))

def seed_database(db: Session):
    print("DEBUG: Seeding database with 100 vehicles...")
    
    fleet_data = [
        ("Ford", ["F-150", "Mustang", "Explorer", "Bronco", "Ranger"]),
        ("Toyota", ["Camry", "Corolla", "RAV4", "Tacoma", "Tundra"]),
        ("Chevrolet", ["Silverado", "Malibu", "Tahoe", "Equinox"]),
        ("Honda", ["Civic", "Accord", "CR-V", "Pilot"]),
        ("Tesla", ["Model 3", "Model Y", "Model S", "Cybertruck"]),
        ("Rivian", ["R1T", "R1S"]),
        ("Dodge", ["Ram 1500", "Charger", "Challenger"])
    ]
    colors = ["White", "Black", "Silver", "Red", "Blue", "Grey", "Green", "Yellow"]
    
    vehicles_to_add = []
    
    for _ in range(100):
        make_tuple = random.choice(fleet_data)
        make = make_tuple[0]
        model = random.choice(make_tuple[1])
        year = random.randint(2015, 2025)
        is_active = random.random() > 0.1 
        
        vehicle = VehicleModel(
            vin=generate_vin(),
            color=random.choice(colors),
            make=make,
            model=model,
            year=year,
            is_active=is_active,
            is_deleted=False,
            last_service_date=datetime.utcnow() - timedelta(days=random.randint(1, 365))
        )
        db.add(vehicle)
        vehicles_to_add.append(vehicle)
    
    db.commit()
    
    # Add maintenance for inactive vehicles
    for v in vehicles_to_add:
        if not v.is_active:
            maint = MaintenanceModel(
                vehicle_id=v.id,
                technician=random.choice(["Mike S.", "Sarah J.", "Tom B."]),
                maintenance_type_id=random.choice([1, 2, 3]),
                status_id=MaintenanceStatus.IN_PROGRESS,
                notes_open="Routine check triggered during seeding.",
                created_on=datetime.utcnow() - timedelta(days=random.randint(0, 5))
            )
            db.add(maint)
    
    db.commit()
    print("DEBUG: Seeding complete!")

# --- 4. APP & ENDPOINTS ---
app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    seed_database(db)
    db.close()

# DTOs
class VehicleDTO(BaseModel):
    id: int
    vin: str    
    color: str  
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
    notesOpen: Optional[str] = Field(None, alias="notes_open")   
    notesClose: Optional[str] = Field(None, alias="notes_close") 
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

# --- 5. TRIGGER SYNC ---
def trigger_sync(vehicle_id: int = None, maintenance_id: int = None, part_order_id: int = None):
    payload = {}
    if vehicle_id: payload["vehicleIds"] = [vehicle_id]
    if maintenance_id: payload["maintenanceIds"] = [maintenance_id]
    if part_order_id: payload["partOrderIds"] = [part_order_id]

    if not payload: return 

    try:
        print(f"Triggering Appian Sync Dispatcher: {payload}")
        requests.post(
            APPIAN_SYNC_URL,
            json=payload,
            headers={"Appian-API-Key": APPIAN_API_KEY},
            timeout=3
        )
    except Exception as e:
        print(f"Failed to trigger sync: {e}")

# --- 6. REQUEST MODELS ---
class CreateVehicleRequest(BaseModel):
    vin: str    
    color: str  
    make: str
    model: str
    year: int

class StartMaintenanceRequest(BaseModel):
    vehicleId: int
    technician: str
    maintenanceTypeId: int
    notesOpen: Optional[str] = None 

class CompleteMaintenanceRequest(BaseModel):
    notesClose: Optional[str] = None 

class OrderPartsRequest(BaseModel):
    maintenanceId: int
    purchaseCardNum: str
    totalAmount: float

# --- 7. ENDPOINTS ---

@app.get("/vehicles/", response_model=List[VehicleDTO])
def get_vehicles(startIndex: int = 0, batchSize: int = 100, ids: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(VehicleModel)
    if ids:
        try:
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(VehicleModel.id.in_(id_list))
        except ValueError: pass 
    return query.offset(startIndex).limit(batchSize).all()

@app.get("/maintenance/", response_model=List[MaintenanceDTO])
def get_maintenance(startIndex: int = 0, batchSize: int = 100, ids: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(MaintenanceModel)
    if ids:
        try:
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(MaintenanceModel.id.in_(id_list))
        except ValueError: pass
    return query.offset(startIndex).limit(batchSize).all()

@app.get("/part-orders/", response_model=List[PartOrderDTO])
def get_part_orders(startIndex: int = 0, batchSize: int = 100, ids: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(PartOrderModel)
    if ids:
        try:
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(PartOrderModel.id.in_(id_list))
        except ValueError: pass
    return query.offset(startIndex).limit(batchSize).all()

# --- NEW HIERARCHICAL SYNC ENDPOINT (FOR APPIAN DATA FABRIC) ---
@app.get("/fleet-fabric/sync")
def get_hierarchical_fleet(startIndex: int = 0, batchSize: int = 50, db: Session = Depends(get_db)):
    # 1. Get total vehicle count
    total_count = db.query(VehicleModel).count()
    
    # 2. FIXED: Use 'maintenance_logs' to match your VehicleModel class
    fleet_data = db.query(VehicleModel).options(
        joinedload(VehicleModel.maintenance_logs) 
        .joinedload(MaintenanceModel.part_orders)
    ).offset(startIndex).limit(batchSize).all()

    # 3. Return the nested structure
    return {
        "data": [
            {
                "id": v.id,
                "vin": v.vin,
                "make": v.make,
                "model": v.model,
                # FIXED: Loop through 'maintenance_logs'
                "maintenance": [
                    {
                        "id": m.id,
                        "technician": m.technician,
                        "statusId": m.status_id,
                        "part_orders": [
                            {"id": p.id, "part": p.purchase_card_num, "cost": p.total_amount}
                            for p in m.part_orders
                        ]
                    } for m in v.maintenance_logs 
                ]
            } for v in fleet_data
        ],
        "totalCount": total_count
    }

@app.post("/vehicles/", response_model=VehicleDTO)
def create_vehicle(vehicle: CreateVehicleRequest, db: Session = Depends(get_db)):
    new_vehicle = VehicleModel(
        vin=vehicle.vin,
        color=vehicle.color,
        make=vehicle.make,
        model=vehicle.model,
        year=vehicle.year,
        is_active=True,
        is_deleted=False
    )
    db.add(new_vehicle)
    db.commit()
    db.refresh(new_vehicle)
    trigger_sync(vehicle_id=new_vehicle.id)
    return new_vehicle

@app.put("/vehicles/{vehicle_id}/retire", response_model=VehicleDTO)
def retire_vehicle(vehicle_id: int, db: Session = Depends(get_db)):
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == vehicle_id).first()
    if not vehicle: raise HTTPException(404, "Vehicle not found")
    vehicle.is_deleted = True
    vehicle.is_active = False 
    db.commit()
    db.refresh(vehicle)
    trigger_sync(vehicle_id=vehicle.id)
    return vehicle

@app.post("/maintenance/start", response_model=MaintenanceDTO)
def start_maintenance(req: StartMaintenanceRequest, db: Session = Depends(get_db)):
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == req.vehicleId).first()
    if not vehicle: raise HTTPException(404, "Vehicle not found")
    
    new_maint = MaintenanceModel(
        vehicle_id=req.vehicleId,
        technician=req.technician,
        maintenance_type_id=req.maintenanceTypeId,
        status_id=MaintenanceStatus.IN_PROGRESS,
        notes_open=req.notesOpen,
        created_on=datetime.utcnow()
    )
    db.add(new_maint)
    vehicle.is_active = False 
    db.commit()
    db.refresh(new_maint)
    trigger_sync(maintenance_id=new_maint.id) 
    return new_maint

@app.post("/maintenance/parts", response_model=PartOrderDTO)
def order_parts(req: OrderPartsRequest, db: Session = Depends(get_db)):
    maint = db.query(MaintenanceModel).filter(MaintenanceModel.id == req.maintenanceId).first()
    if not maint: raise HTTPException(404, "Maintenance record not found")
    
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
    trigger_sync(part_order_id=new_order.id)
    return new_order

@app.put("/maintenance/{maintenance_id}/complete", response_model=MaintenanceDTO)
def complete_maintenance(maintenance_id: int, req: CompleteMaintenanceRequest, db: Session = Depends(get_db)):
    maint = db.query(MaintenanceModel).filter(MaintenanceModel.id == maintenance_id).first()
    if not maint: raise HTTPException(404, "Maintenance record not found")
    vehicle = db.query(VehicleModel).filter(VehicleModel.id == maint.vehicle_id).first()

    maint.status_id = MaintenanceStatus.COMPLETED
    maint.completed_on = datetime.utcnow()
    maint.notes_close = req.notesClose
    
    if vehicle:
        vehicle.is_active = True
        vehicle.last_service_date = datetime.utcnow()
        
    db.commit()
    db.refresh(maint)
    trigger_sync(maintenance_id=maint.id)
    return maint

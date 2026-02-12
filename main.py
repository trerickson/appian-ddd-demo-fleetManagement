import os
import sys
import random
import string
import requests
from datetime import datetime, timedelta
from typing import List, Optional
from enum import IntEnum

from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, ForeignKey, func, text
from sqlalchemy.orm import sessionmaker, relationship, Session, declarative_base, joinedload

# --- 1. CONFIGURATION & CONNECTION ---
raw_url = os.getenv("DATABASE_URL")

if not raw_url:
    print("FATAL ERROR: DATABASE_URL environment variable is MISSING.")
    sys.exit(1)

if raw_url.startswith("postgres://"):
    raw_url = raw_url.replace("postgres://", "postgresql://", 1)

engine = create_engine(raw_url)
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
    
    # Relationship for Hierarchical Read
    maintenance_logs = relationship("MaintenanceModel", back_populates="vehicle")

class MaintenanceModel(Base):
    __tablename__ = "fm_maintenances"
    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(Integer, ForeignKey("fm_vehicles.id"))
    technician = Column(String)
    maintenance_type_id = Column(Integer)
    status_id = Column(Integer, default=MaintenanceStatus.IN_PROGRESS)
    notes_open = Column(String, nullable=True)   
    notes_close = Column(String, nullable=True)  
    created_on = Column(DateTime, default=datetime.utcnow)
    completed_on = Column(DateTime, nullable=True)

    vehicle = relationship("VehicleModel", back_populates="maintenance_logs")
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

# --- 3. DATABASE SETUP & SEEDING ---
# Ensure tables are created
Base.metadata.create_all(bind=engine)

def generate_vin():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=17))

def seed_database(db: Session):
    print("DEBUG: Checking if seeding is needed...")
    if db.query(VehicleModel).count() > 0:
        return
    
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
        v = VehicleModel(
            vin=generate_vin(),
            color=random.choice(colors),
            make=make_tuple[0],
            model=random.choice(make_tuple[1]),
            year=random.randint(2015, 2025),
            is_active=random.random() > 0.1,
            is_deleted=False,
            last_service_date=datetime.utcnow() - timedelta(days=random.randint(1, 365))
        )
        db.add(v)
        vehicles_to_add.append(v)
    db.commit()
    print("DEBUG: Seeding complete!")

# --- 4. APP & DEPENDENCIES ---
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

# --- 5. DTOs ---
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

# --- 6. APPIAN SYNC TRIGGER ---
def trigger_sync(vehicle_id: int = None, maintenance_id: int = None, part_order_id: int = None):
    payload = {}
    if vehicle_id: payload["vehicleIds"] = [vehicle_id]
    if maintenance_id: payload["maintenanceIds"] = [maintenance_id]
    if part_order_id: payload["partOrderIds"] = [part_order_id]
    if not payload: return 
    try:
        requests.post(APPIAN_SYNC_URL, json=payload, headers={"Appian-API-Key": APPIAN_API_KEY}, timeout=3)
    except Exception as e:
        print(f"Sync trigger failed: {e}")

# --- 7. REQUEST MODELS ---
class CreateVehicleRequest(BaseModel):
    vin: str; color: str; make: str; model: str; year: int
class StartMaintenanceRequest(BaseModel):
    vehicleId: int; technician: str; maintenanceTypeId: int; notesOpen: Optional[str] = None 
class CompleteMaintenanceRequest(BaseModel):
    notesClose: Optional[str] = None 
class OrderPartsRequest(BaseModel):
    maintenanceId: int; purchaseCardNum: str; totalAmount: float

# --- 8. HIERARCHICAL SYNC ENDPOINT (NET NEW) ---
@app.get("/fleet-fabric/sync")
def get_hierarchical_fleet(startIndex: int = 0, batchSize: int = 50, db: Session = Depends(get_db)):
    """Orchestrator for Appian Service-Backed Sync."""
    try:
        total_count = db.query(VehicleModel).count()
        fleet_data = db.query(VehicleModel).options(
            joinedload(VehicleModel.maintenance_logs).joinedload(MaintenanceModel.part_orders)
        ).offset(startIndex).limit(batchSize).all()
        return {
            "data": [
                {
                    "id": v.id, "vin": v.vin, "make": v.make, "model": v.model,
                    "maintenance": [
                        {
                            "id": m.id, "technician": m.technician, "statusId": m.status_id,
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- 9. ATOMIC ENDPOINTS (LEGACY) ---
@app.get("/vehicles/", response_model=List[VehicleDTO])
def get_vehicles(startIndex: int = 0, batchSize: int = 100, ids: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(VehicleModel)
    if ids:
        id_list = [int(i) for i in ids.split(",")]
        query = query.filter(VehicleModel.id.in_(id_list))
    return query.offset(startIndex).limit(batchSize).all()

@app.get("/maintenance/", response_model=List[MaintenanceDTO])
def get_maintenance(startIndex: int = 0, batchSize: int = 100, ids: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(MaintenanceModel)
    if ids:
        id_list = [int(i) for i in ids.split(",")]
        query = query.filter(MaintenanceModel.id.in_(id_list))
    return query.offset(startIndex).limit(batchSize).all()

@app.get("/part-orders/", response_model=List[PartOrderDTO])
def get_part_orders(startIndex: int = 0, batchSize: int = 100, ids: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(PartOrderModel)
    if ids:
        id_list = [int(i) for i in ids.split(",")]
        query = query.filter(PartOrderModel.id.in_(id_list))
    return query.offset(startIndex).limit(batchSize).all()

@app.post("/vehicles/", response_model=VehicleDTO)
def create_vehicle(vehicle: CreateVehicleRequest, db: Session = Depends(get_db)):
    new_v = VehicleModel(**vehicle.dict(), is_active=True, is_deleted=False)
    db.add(new_v); db.commit(); db.refresh(new_v)
    trigger_sync(vehicle_id=new_v.id); return new_v

@app.put("/vehicles/{v_id}/retire", response_model=VehicleDTO)
def retire_vehicle(v_id: int, db: Session = Depends(get_db)):
    v = db.query(VehicleModel).filter(VehicleModel.id == v_id).first()
    if not v: raise HTTPException(404, "Not found")
    v.is_deleted = True; v.is_active = False; db.commit(); db.refresh(v)
    trigger_sync(vehicle_id=v.id); return v

@app.post("/maintenance/start", response_model=MaintenanceDTO)
def start_maintenance(req: StartMaintenanceRequest, db: Session = Depends(get_db)):
    v = db.query(VehicleModel).filter(VehicleModel.id == req.vehicleId).first()
    if not v: raise HTTPException(404, "Not found")
    m = MaintenanceModel(vehicle_id=req.vehicleId, technician=req.technician, maintenance_type_id=req.maintenanceTypeId, created_on=datetime.utcnow())
    db.add(m); v.is_active = False; db.commit(); db.refresh(m)
    trigger_sync(maintenance_id=m.id); return m

@app.post("/maintenance/parts", response_model=PartOrderDTO)
def order_parts(req: OrderPartsRequest, db: Session = Depends(get_db)):
    m = db.query(MaintenanceModel).filter(MaintenanceModel.id == req.maintenanceId).first()
    if not m: raise HTTPException(404, "Not found")
    p = PartOrderModel(maintenance_id=req.maintenanceId, purchase_card_num=req.purchaseCardNum, total_amount=req.totalAmount)
    db.add(p); m.status_id = MaintenanceStatus.WAITING_FOR_PARTS; db.commit(); db.refresh(p)
    trigger_sync(part_order_id=p.id); return p

@app.put("/maintenance/{m_id}/complete", response_model=MaintenanceDTO)
def complete_maintenance(m_id: int, req: CompleteMaintenanceRequest, db: Session = Depends(get_db)):
    m = db.query(MaintenanceModel).filter(MaintenanceModel.id == m_id).first()
    if not m: raise HTTPException(404, "Not found")
    v = db.query(VehicleModel).filter(VehicleModel.id == m.vehicle_id).first()
    m.status_id = MaintenanceStatus.COMPLETED; m.completed_on = datetime.utcnow(); m.notes_close = req.notesClose
    if v: v.is_active = True; v.last_service_date = datetime.utcnow()
    db.commit(); db.refresh(m); trigger_sync(maintenance_id=m.id); return m

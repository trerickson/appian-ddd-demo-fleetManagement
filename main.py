import os
import sys
import random
import string
from typing import List, Optional
from enum import IntEnum

from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, ForeignKey, func, text
from sqlalchemy.orm import sessionmaker, relationship, Session, declarative_base

# --- 1. THE CRASH TEST CONFIGURATION ---
# NO FALLBACK. If DATABASE_URL is missing, we crash.
raw_url = os.getenv("DATABASE_URL")

print("----------------------------------------------------------------")
print(f"DEBUG: Checking for DATABASE_URL...")

if not raw_url:
    print("FATAL ERROR: DATABASE_URL environment variable is MISSING.")
    print("The app cannot connect to Postgres.")
    print("----------------------------------------------------------------")
    sys.exit(1) # Force crash

# Mask the password for logs, but show the host
masked_url = raw_url.split("@")[-1] if "@" in raw_url else "INVALID_FORMAT"
print(f"DEBUG: Found URL pointing to: {masked_url}")

# Fix for Railway/Heroku "postgres://" vs "postgresql://"
if raw_url.startswith("postgres://"):
    raw_url = raw_url.replace("postgres://", "postgresql://", 1)
    print("DEBUG: Fixed URL scheme to postgresql://")

print("----------------------------------------------------------------")

# --- 2. SETUP DB ---
try:
    engine = create_engine(raw_url)
    # Test the connection immediately
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print("SUCCESS: Connection to Postgres established!")
except Exception as e:
    print(f"FATAL CONNECTION ERROR: {e}")
    sys.exit(1) # Force crash

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 3. MODELS ---
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
    status_id = Column(Integer, default=1)
    notes_open = Column(String, nullable=True)
    notes_close = Column(String, nullable=True)
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

# --- 4. RESET & SEED ---
print("DEBUG: Starting Database Reset...")
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)
print("DEBUG: Tables Dropped and Recreated.")

def generate_vin():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=17))

def seed_database(db: Session):
    print("DEBUG: Starting Seeder...")
    fleet_data = [
        ("Ford", ["F-150", "Mustang", "Explorer", "Bronco", "Ranger"]),
        ("Toyota", ["Camry", "Corolla", "RAV4", "Tacoma", "Tundra"]),
        ("Chevrolet", ["Silverado", "Malibu", "Tahoe", "Equinox"]),
        ("Tesla", ["Model 3", "Model Y", "Model S", "Cybertruck"])
    ]
    colors = ["White", "Black", "Silver", "Red", "Blue", "Grey"]

    vehicles_to_add = []
    
    # Create 100 Vehicles
    for _ in range(100):
        make_tuple = random.choice(fleet_data)
        make = make_tuple[0]
        model = random.choice(make_tuple[1])
        vehicle = VehicleModel(
            vin=generate_vin(),
            color=random.choice(colors),
            make=make,
            model=model,
            year=random.randint(2015, 2025),
            is_active=True,
            is_deleted=False,
            last_service_date=datetime.utcnow()
        )
        db.add(vehicle)
        vehicles_to_add.append(vehicle)
    
    db.commit()
    
    # Create Maintenance
    for v in vehicles_to_add[:20]: # Add maintenance to first 20 cars
        maint = MaintenanceModel(
            vehicle_id=v.id,
            technician="Auto-Seed",
            maintenance_type_id=1,
            status_id=1,
            notes_open="Seeded Maintenance",
            created_on=datetime.utcnow()
        )
        db.add(maint)
    
    db.commit()
    print("DEBUG: Seeding Complete! (100 Vehicles, 20 Maintenance records)")

# --- 5. APP ---
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

# --- 6. READ ENDPOINTS ---
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

@app.get("/vehicles/", response_model=List[VehicleDTO])
def get_vehicles(startIndex: int = 0, batchSize: int = 100, ids: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(VehicleModel)
    if ids:
        try:
            id_list = [int(i) for i in ids.split(",")]
            query = query.filter(VehicleModel.id.in_(id_list))
        except ValueError: pass
    return query.offset(startIndex).limit(batchSize).all()

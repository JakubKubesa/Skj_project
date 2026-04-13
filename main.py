import os
import shutil
from fastapi import FastAPI, UploadFile, Depends, HTTPException
from fastapi.responses import FileResponse as FastAPIFileResponse
from sqlalchemy.orm import Session
from database import engine, get_db, Base
import models
import schemas

# Vytvoření tabulek v DB
models.Base.metadata.create_all(bind=engine)

app = FastAPI()

STORAGE_DIR = "storage"

@app.post("/files/upload", response_model=schemas.FileResponse)
async def upload_file(user_id: str, file: UploadFile, db: Session = Depends(get_db)):
    # 1. Cesta pro uložení
    user_dir = os.path.join(STORAGE_DIR, user_id)
    os.makedirs(user_dir, exist_ok=True)
    
    file_path = os.path.join(user_dir, file.filename)
    
    # 2. Uložení fyzického souboru
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # 3. Uložení metadat do DB
    db_file = models.FileModel(
        user_id=user_id,
        filename=file.filename,
        path=file_path,
        size=os.path.getsize(file_path)
    )
    db.add(db_file)
    db.commit()
    db.refresh(db_file)
    
    return db_file

@app.get("/files/{file_id}")
async def get_file(file_id: str, db: Session = Depends(get_db)): 
    db_file = db.query(models.FileModel).filter(models.FileModel.id == file_id).first()
    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")
    
    return FastAPIFileResponse(path=db_file.path, filename=db_file.filename)

@app.delete("/files/{file_id}")
async def delete_file(file_id: str, db: Session = Depends(get_db)): # Změněno na str
    db_file = db.query(models.FileModel).filter(models.FileModel.id == file_id).first()
    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Smazání z disku
    if os.path.exists(db_file.path):
        os.remove(db_file.path)
    
    # Smazání z DB
    db.delete(db_file)
    db.commit()
    
    return {"message": "File deleted successfully"}
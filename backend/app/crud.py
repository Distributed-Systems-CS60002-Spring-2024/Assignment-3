from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from .database import engine
Base = automap_base()
Base.prepare(engine,reflect=True)
def read(db: Session,shard:str,low:int=0,high:int=2**63-1):
    model=getattr(Base.classes,shard)
    return db.query(model).filter(model.Stud_id.between(low,high)).all()
def write(db: Session,shard:str,data:list):
    model=getattr(Base.classes,shard)
    for student in data:
        if(db.query(model).filter(model.Stud_id==student.Stud_id).first()):
            print("Model with student id",student.Stud_id,"already exist")
            return student.Stud_id
        student=dict(student)
        new_student=model(**student)
        db.add(new_student)
    db.commit()
    return -1
def update(db:Session,shard:str,Stud_id:int,data):
    model=getattr(Base.classes,shard)
    student=db.query(model).filter(model.Stud_id==Stud_id).first()
    if student:
        for k,v in data:
            setattr(student,k,v)
        db.commit()
        return 1
    else:
        return -1
def delete(db:Session,shard:str,Stud_id:int):
    model=getattr(Base.classes,shard)
    student=db.query(model).filter(model.Stud_id==Stud_id).first()
    if student:
        db.delete(student)
        db.commit()
        return 1
    else:
        return -1






from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Annotated
import models
from database import engine, SessionLocal
from sqlalchemy.orm import Session
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd

cred = credentials.Certificate('restau-5dba7-firebase-adminsdk-jpame-be78ad3e26.json')
app = firebase_admin.initialize_app(cred)
firestoreDB = firestore.client()

app = FastAPI()
models.Base.metadata.create_all(bind=engine)

"""
class ChoiceBase(BaseModel):
    choice_text: str
    is_correct: bool


class QuestionBase(BaseModel):
    question_text: str
    choices: List[ChoiceBase]
""" 

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

db_dependency = Annotated[Session, Depends(get_db)]

"""
@app.get("/questions/{question_id}")
async def read_question(question_id: int, db: db_dependency):
    result = db.query(models.Questions).filter(models.Questions.id == question_id).first()
    if not result:
        raise HTTPException(status_code=404, detail="Question not found")
    else:
        return result


@app.post("/questions/")
async def create_questions(question: QuestionBase, db: db_dependency):
    db_question = models.Questions(question_text = question.question_text)
    db.add(db_question)
    db.commit()
    db.refresh(db_question)
    for choice in question.choices:
        db_choice = models.Choices(choice_text = choice.choice_text, is_correct = choice.is_correct, question_id = db_question.id)
        db.add(db_choice)
        db.commit()
"""

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/screentimes/clean")
async def root(db: db_dependency):
    db.query(models.LastVisit).delete()
    db.query(models.WeekScreenTime).delete()
    db_visit = models.LastVisit(id = 1, year = 0, week = 0)
    db.merge(db_visit)
    db.commit()
    return {
        "response": "Database Successfully cleaned"
    }

@app.get("/screentimes")
async def setup(db: db_dependency):

    #Retriving cached moment of visit
    visitRetrieved = db.query(models.LastVisit).filter(models.LastVisit.id == 1).first()

    latestWeek = visitRetrieved.week
    latestYear = visitRetrieved.year

    #Getting current moment
    currentWeek = pd.Timestamp.now().week
    currentYear = pd.Timestamp.now().year

    #Checking if the cached answer is to be used
    if (latestYear != currentYear or latestWeek != currentWeek):

        print("Retrieving answer...")

        #Firebase events retrieval
        docs = firestoreDB.collection("screen_time_events").get()

        #Turning documents into a pandas dataframe
        dictArray = []
        for doc in docs:
            dictArray.append(doc.to_dict())
        df = pd.DataFrame(dictArray)

        #Ordering and getting the necessary data
        df["week"] = df["date"].transform(lambda date: date.week)
        df["year"] = df["date"].transform(lambda date: date.year)
        df["week"] = df["year"].astype(str) + "-" + df["week"].astype(str)
        cols = ["screen", "user_id", "week", "time_seconds"]
        df = df[cols]

        #Performing the analysis process
        grouped = df.groupby(["screen", "user_id", "week"])
        firstgrouped = grouped.sum()
        grouped = firstgrouped.groupby(["screen", "user_id"])
        secondgrouped = grouped.mean()
        grouped = secondgrouped.groupby("screen")
        answer = grouped.mean()
        #Saving the answer in local storage
        for index, row in answer.iterrows():
            db_screen = models.WeekScreenTime(screen = index, time = float(row["time_seconds"]))
            db.merge(db_screen)
            db.commit()
    
    #Lastest visit update
    db_visit = models.LastVisit(id = 1, year = currentYear, week = currentWeek)
    db.merge(db_visit)
    db.commit()

    #Answer building
    answer = {}
    dataRetrieval = db.query(models.WeekScreenTime).order_by(models.WeekScreenTime.time).all()
    for entry in dataRetrieval:
        answer[entry.screen] = entry.time
    return answer
    
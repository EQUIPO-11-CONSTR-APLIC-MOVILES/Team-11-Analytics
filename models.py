from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Float
from database import Base


class WeekScreenTime(Base):
    __tablename__ = 'weekscreentime'
    screen = Column(String, primary_key=True, index = True)
    time = Column(Float, index = True)

class LastVisit(Base):
    __tablename__ = 'lastvisit'
    id = Column(Integer, primary_key=True, index = True)
    year = Column(Integer, index = True)
    week = Column(Integer, index = True)



"""
class Questions(Base):
    __tablename__ = 'questions'

    id = Column(Integer, primary_key = True, index = True)
    question_text = Column(String, index = True)

class Choices(Base):
    __tablename__ = 'choices'

    id = Column(Integer, primary_key = True, index = True)
    choice_text = Column(String, index = True)
    is_correct = Column(Boolean, default = False)
    question_id = Column(Integer, ForeignKey("questions.id"))
"""
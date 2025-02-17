from sqlalchemy import TIMESTAMP, Column, Float, ForeignKey, Integer, String, Text
from sqlalchemy import func
from sqlalchemy.orm import relationship
from database import Base, SessionLocal, engine


class User(Base):
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True, name="id")
    gender = Column(Integer, name="gender")
    age = Column(Integer, name="age")
    country = Column(Text, name="country")
    city = Column(Text, name="city")
    exp_group = Column(Integer, name="exp_group")
    os = Column(Text, name="os")
    source = Column(Text, name="source")


if __name__ == "__main__":
    with SessionLocal() as session:
        results = (
            session.query(User.country, User.os, func.count())
            .filter(User.exp_group == 3)
            .group_by(User.country, User.os)
            .having(func.count() > 100)
            .order_by(func.count().desc())
            .all()
    )

    answer = []
    for i in range(len(results)):
        answer.append(results[i])
    print(answer)

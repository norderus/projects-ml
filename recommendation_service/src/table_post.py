from sqlalchemy import TIMESTAMP, Column, Float, ForeignKey, Integer, String, Text
from sqlalchemy import func
from sqlalchemy.orm import relationship
from database import Base, SessionLocal, engine


class Post(Base):
    __tablename__ = 'post'

    id = Column(Integer, primary_key=True, name="id")
    text = Column(Text, name="text")
    topic = Column(Text, name="topic")


if __name__ == "__main__":      
    with SessionLocal() as session:
        results = (
            session.query(Post)
            .filter(Post.topic == 'business')
            .order_by(Post.id.desc())
            .limit(10)
            .all()
        )

    answer = []
    for i in range(len(results)):
        answer.append(results[i][0])
    print(answer)

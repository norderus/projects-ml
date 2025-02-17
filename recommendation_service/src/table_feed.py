from sqlalchemy import TIMESTAMP, Column, Float, ForeignKey, Integer, String, Text
from sqlalchemy import func
from sqlalchemy.orm import relationship
from database import Base, SessionLocal, engine
from table_post import Post
from table_user import User


class Feed(Base):
    __tablename__ = 'feed_action'

    user_id = Column(Integer, ForeignKey(User.id), primary_key=True, name="user_id")
    user = relationship(User)
    post_id = Column(Integer, ForeignKey(Post.id), primary_key=True, name="post_id")
    post = relationship(Post)
    action = Column(Text, name="action")
    time = Column(TIMESTAMP, name="time")

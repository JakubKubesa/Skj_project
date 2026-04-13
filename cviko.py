from sqlalchemy import ForeignKey, create_engine, Column, Integer, String, select
from sqlalchemy.orm import declarative_base, Session, Mapped, mapped_column, relationship

engine = create_engine('sqlite:///users.db', echo=True)

class Base(declarative_base()):
    pass

class User(Base):
    __tablename__ = 'users'
    
    id = Mapped[int] = mapped_column(Integer, primary_key=True)
    name = Mapped[str] = mapped_column(String(50), nullable=False)
    email = Mapped[str] = mapped_column(String(100))
    posts: Mapped[list["Post"]] = relationship(back_populates='author')

    def __repr__(self):
        return f"User(id={self.id}, name='{self.name}', email='{self.email}')"
    
    
class Post(Base):
    __tablename__ = 'posts'
    
    id = Mapped[int] = mapped_column(Integer, primary_key=True)
    title = Mapped[str] = mapped_column(String(100), nullable=False)
    content = Mapped[str] = mapped_column(String(500))
    
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))

    author: Mapped[User] = relationship(back_populates='posts')
    
    def __repr__(self):
        return f"Post(id={self.id}, title='{self.title}', content='{self.content}')"
    
Base.metadata.create_all(engine)

with Session(engine) as session:
    user1 = User(name='Alice', email= 'alice@vsb.cz');
    user2 = User(name='Bob', email= 'bob@vsb.cz');
    session.add(user1)
    session.add(user2)
    session.commit()
    
    
with Session(engine) as session:
    stmt = select(User)
    
    res = session.execute(stmt).scalars().all()
    
    for user in res:
        print(user)


with Session(engine) as session:
    stmt = select(User).where(User.email == 'alice@vsb.cz')
    res = session.execute(stmt).scalars().all()
    user.name = 'Alice Smith'
    session.commit()
    
with Session(engine) as session:
    stmt = select(User)
    
    res = session.execute(stmt).scalars().all()
    
    









with Session(engine) as session:
    posts = [
        Post(title='First Post', content='This is the first post', author=user1),
        Post(title='Second Post', content='This is the second post', author=user1)
    ]
    stmt = select(User).where(User.email == 'alice@vsb.cz')
    user = session.execute(stmt).scalar_one()
    
    user.posts.extend(posts)
    session.commit()
    
with Session(engine) as session:
    stmst = select(User).where(User.email == 'alice@vsb.cz')
    user = session.execute(stmst).scalar_one()
    
    for post in user.posts:
        print(post)
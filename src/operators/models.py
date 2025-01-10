from sqlalchemy import Column, Integer, Float, Text, String, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Tabla intermedia para la relación muchos-a-muchos entre TVShow y Genre
show_genre_association = Table(
    'show_genre', Base.metadata,
    Column('tv_show_id', Integer, ForeignKey('tv_shows.id'), primary_key=True),
    Column('genre_id', Integer, ForeignKey('genres.id'), primary_key=True)
)


class TVShow(Base):
    """
    Modelo SQLAlchemy para la tabla de shows de TV.
    """
    __tablename__ = 'tv_shows'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=True)
    language = Column(String, nullable=True)
    official_site = Column(String, nullable=True)
    runtime = Column(Integer, nullable=True)
    premiered = Column(String, nullable=True)
    ended = Column(String, nullable=True)
    rating_average = Column(Float, nullable=True)
    summary = Column(Text, nullable=True)

    # Relación muchos-a-muchos con géneros
    genres = relationship("Genre", secondary=show_genre_association, back_populates="shows")

    def __repr__(self):
        return f"<TVShow(name={self.name}, runtime={self.runtime})>"


class Genre(Base):
    """
    Modelo SQLAlchemy para la tabla de géneros.
    """
    __tablename__ = 'genres'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)

    # Relación muchos-a-muchos con shows
    shows = relationship("TVShow", secondary=show_genre_association, back_populates="genres")

    def __repr__(self):
        return f"<Genre(name={self.name})>"


from sqlalchemy import create_engine
from src.operators.path_manager import PathManager

engine = create_engine(f"sqlite:///{PathManager().get_db_path()}")

# Crear todas las tablas
Base.metadata.create_all(engine)

print("Tablas creadas exitosamente.")


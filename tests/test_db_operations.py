# tests/test_db_operations.py

import pytest
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

# Define the base class and engine
Base = declarative_base()

class Hotel(Base):
    __tablename__ = 'Hotels'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)

class Room(Base):
    __tablename__ = 'Rooms'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    room_type = Column(String)
    floor = Column(Integer)
    hotel_id = Column(Integer, ForeignKey('Hotels.id'))
    hotel = relationship("Hotel")

class User(Base):
    __tablename__ = 'Users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    birth_date = Column(DateTime)
    gender = Column(String)
    email = Column(String)
    phoneNumber = Column(String)

class Stay(Base):
    __tablename__ = 'Stays'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    reference_reservation_id = Column(Integer, ForeignKey('Reservations.id'))
    room_id = Column(Integer, ForeignKey('Rooms.id'))
    guest_id = Column(Integer, ForeignKey('Users.id'))
    reservation = relationship("Reservation")
    room = relationship("Room")
    guest = relationship("User")

class Reservation(Base):
    __tablename__ = 'Reservations'
    id = Column(Integer, primary_key=True)
    reservation_datetime = Column(DateTime)
    check_in_date = Column(DateTime)
    check_out_date = Column(DateTime)
    status = Column(String)
    hotel_id = Column(Integer, ForeignKey('Hotels.id'))
    booker_id = Column(Integer, ForeignKey('Users.id'))
    total_room_price = Column(Float)
    voucher_code = Column(String)
    total_discount = Column(Float)
    hotel = relationship("Hotel")
    booker = relationship("User")

class ReservationItem(Base):
    __tablename__ = 'ReservationItems'
    id = Column(Integer, primary_key=True)
    reservation_id = Column(Integer, ForeignKey('Reservations.id'))
    reservation_datetime = Column(DateTime)
    check_in_date = Column(DateTime)
    check_out_date = Column(DateTime)
    room_type = Column(String)
    total_room_price = Column(Float)
    total_discount = Column(Float)
    reservation = relationship("Reservation")

@pytest.fixture(scope="function")
def db_session():
    # Setup: Create an in-memory SQLite database and configure SQLAlchemy
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

def test_insert_hotels(db_session):
    # Insert data into the Hotels table
    db_session.add_all([
        Hotel(id=1, name='Hotel A', type='Pod'),
        Hotel(id=2, name='Hotel B', type='Cabin')
    ])
    db_session.commit()

    # Query and test the data
    hotels = db_session.query(Hotel).all()
    assert len(hotels) == 2
    assert hotels[0].name == 'Hotel A'
    assert hotels[1].name == 'Hotel B'

def test_insert_rooms(db_session):
    # Insert data into the Rooms table
    db_session.add_all([
        Room(id=1, name='Room 101', room_type='Single', floor=1, hotel_id=1),
        Room(id=2, name='Room 102', room_type='Double', floor=1, hotel_id=1),
        Room(id=3, name='Room 201', room_type='Suite', floor=2, hotel_id=2)
    ])
    db_session.commit()

    # Query and test the data
    rooms = db_session.query(Room).all()
    assert len(rooms) == 3
    assert rooms[0].name == 'Room 101'
    assert rooms[1].name == 'Room 102'
    assert rooms[2].name == 'Room 201'

def test_insert_users(db_session):
    # Insert data into the Users table
    db_session.add_all([
        User(id=1, name='Alice Smith', birth_date=datetime(1990, 1, 15), gender='Female', email='alice@example.com', phoneNumber='+62-8123456789'),
        User(id=2, name='Bob Johnson', birth_date=datetime(1985, 2, 20), gender='Male', email='bob@example.com', phoneNumber='+62-8123456790'),
        User(id=3, name='Charlie Brown', birth_date=datetime(1978, 11, 30), gender='Male', email='charlie@example.com', phoneNumber='+62-8123456791'),
        User(id=4, name='Diana Prince', birth_date=datetime(1989, 3, 21), gender='Female', email='diana@example.com', phoneNumber='+62-8123456792'),
        User(id=5, name='Eve Adams', birth_date=datetime(1992, 5, 10), gender='Female', email='eve@example.com', phoneNumber='+62-8123456793'),
        User(id=6, name='Frank Miller', birth_date=datetime(1981, 7, 25), gender='Male', email='frank@example.com', phoneNumber='+62-8123456794'),
        User(id=7, name='Grace Kelly', birth_date=datetime(1984, 9, 13), gender='Female', email='grace@example.com', phoneNumber='+62-8123456795'),
        User(id=8, name='Hank Green', birth_date=datetime(1991, 10, 16), gender='Male', email='hank@example.com', phoneNumber='+62-8123456796'),
        User(id=9, name='Ivy League', birth_date=datetime(1993, 12, 22), gender='Female', email='ivy@example.com', phoneNumber='+62-8123456797'),
        User(id=10, name='Jack Daniels', birth_date=datetime(1988, 6, 5), gender='Male', email='jack@example.com', phoneNumber='+62-8123456798'),
        User(id=11, name='Kim Lee', birth_date=datetime(1987, 8, 12), gender='Female', email='kim@example.com', phoneNumber='+62-8123456799'),
        User(id=12, name='Liam Neeson', birth_date=datetime(1980, 2, 15), gender='Male', email='liam@example.com', phoneNumber='+62-8123456800'),
        User(id=13, name='Mona Lisa', birth_date=datetime(1995, 11, 4), gender='Female', email='mona@example.com', phoneNumber='+62-8123456801'),
        User(id=14, name='Nina Simone', birth_date=datetime(1982, 3, 18), gender='Female', email='nina@example.com', phoneNumber='+62-8123456802'),
        User(id=15, name='Oscar Wilde', birth_date=datetime(1975, 7, 30), gender='Male', email='oscar@example.com', phoneNumber='+62-8123456803')
    ])
    db_session.commit()

    # Query and test the data
    users = db_session.query(User).all()
    assert len(users) == 15

def test_insert_stays(db_session):
    # Insert data into the Stays table
    db_session.add_all([
        Stay(id=1, date=datetime(2024, 6, 16), reference_reservation_id=1001, room_id=1, guest_id=1),
        Stay(id=2, date=datetime(2024, 7, 2), reference_reservation_id=1002, room_id=3, guest_id=2),
        Stay(id=3, date=datetime(2024, 7, 15), reference_reservation_id=1003, room_id=2, guest_id=3),
        Stay(id=4, date=datetime(2024, 8, 1), reference_reservation_id=1004, room_id=1, guest_id=4),
        Stay(id=5, date=datetime(2024, 8, 15), reference_reservation_id=1005, room_id=3, guest_id=5)
    ])
    db_session.commit()

    # Query and test the data
    stays = db_session.query(Stay).all()
    assert len(stays) == 5
    assert stays[0].date == datetime(2024, 6, 16)
    assert stays[1].date == datetime(2024, 7, 2)
    assert stays[2].date == datetime(2024, 7, 15)
    assert stays[3].date == datetime(2024, 8, 1)
    assert stays[4].date == datetime(2024, 8, 15)

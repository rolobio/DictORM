import asyncpg
import pytest

from dictorm.async import DictDB
from .test_dictorm import test_db_login


schema = '''
    CREATE TABLE person (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        other INTEGER,
        manager_id INTEGER REFERENCES person(id)
    );
    CREATE TABLE department (
        id SERIAL PRIMARY KEY,
        name TEXT
    );
    CREATE TABLE person_department (
        person_id INTEGER REFERENCES person(id),
        department_id INTEGER REFERENCES department(id),
        PRIMARY KEY (person_id, department_id)
    );
    CREATE TABLE car (
        id SERIAL PRIMARY KEY,
        license_plate TEXT,
        name TEXT,
        person_id INTEGER REFERENCES person(id)
    );
    ALTER TABLE person ADD COLUMN car_id INTEGER REFERENCES car(id);
    CREATE TABLE no_pk (foo VARCHAR(10));
    CREATE TABLE station (
        person_id INTEGER
    );
    CREATE TABLE possession (
        id SERIAL PRIMARY KEY,
        person_id INTEGER,
        description JSONB
    );
'''

@pytest.mark.asyncio
async def test_one():
    conn = await asyncpg.connect(**test_db_login)
    await conn.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')
    await conn.execute(schema)
    db = DictDB(conn)
    await db.init()
    Person = db['person']
    bob = Person(name='Bob')
    print(bob)
    bob.flush()
    print(bob)
    raise

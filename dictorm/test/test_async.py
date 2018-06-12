import asyncio

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

    # Create a person, their ID should be set after flush
    bob = Person(name='Bob')
    assert bob == {'name': 'Bob'}
    await bob.flush()
    assert set(bob.items()).issuperset({('name', 'Bob'), ('id', 1)})

    # Name change sticks after flush
    bob['name'] = 'Steve'
    await bob.flush()
    assert set(bob.items()).issuperset({('name', 'Steve'), ('id', 1)})

    # Create a second person
    alice = await Person(name='Alice').flush()
    assert set(alice.items()).issuperset({('name', 'Alice'), ('id', 2)})

    # Can get all people
    persons = await Person.get_where()
    for person, expected in zip(persons, [bob, alice]):
        assert person._table == expected._table
        assert person == expected

    # Delete Bob, a single person remains untouched
    await bob.delete()
    persons = list(await Person.get_where())
    assert persons == [alice]
    assert persons[0]['id'] == 2

    # Can get all people
    persons = list(await Person.get_where(Person['id'] == 2))
    assert persons[0]['id'] == 2

    # Create a new person, can use greater-than filter
    steve = await Person(name='Steve').flush()
    persons = list(await Person.get_where(Person['id'] > 2))
    assert [steve] == persons

    # Insert several people
    persons = await asyncio.gather(
        Person(name='Frank').flush(),
        Person(name='Phil').flush(),
        Person(name='Sally').flush(),
    )
    print(persons)

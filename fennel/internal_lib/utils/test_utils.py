from typing import Dict, List

from fennel.dtypes.dtypes import struct


@struct
class Manufacturer:
    name: str
    country: str


@struct
class Car:
    make: Manufacturer
    model: str
    year: int


@struct
class UserCarMap:
    map: Dict[str, List[Car]]


def test_as_json():
    # Create an instance of Manufacturer
    manufacturer1 = Manufacturer("Test Manufacturer", "Test Country")
    # Convert it to a dictionary
    manufacturer1_dict = manufacturer1.as_json()
    # Check the result
    assert manufacturer1_dict == {
        "name": "Test Manufacturer",
        "country": "Test Country",
    }

    # Create an instance of Car
    car1 = Car(make=manufacturer1, model="Test Model", year=2023)
    # Convert it to a dictionary
    car1_dict = car1.as_json()
    # Check the result
    assert car1_dict == {
        "make": {"name": "Test Manufacturer", "country": "Test Country"},
        "model": "Test Model",
        "year": 2023,
    }

    # Create another instance of Manufacturer
    manufacturer2 = Manufacturer("Second Manufacturer", "Second Country")
    # Convert it to a dictionary
    manufacturer2_dict = manufacturer2.as_json()
    # Check the result
    assert manufacturer2_dict == {
        "name": "Second Manufacturer",
        "country": "Second Country",
    }

    # Create another instance of Car
    car2 = Car(make=manufacturer2, model="Second Model", year=2024)
    # Convert it to a dictionary
    car2_dict = car2.as_json()
    # Check the result
    assert car2_dict == {
        "make": {"name": "Second Manufacturer", "country": "Second Country"},
        "model": "Second Model",
        "year": 2024,
    }

    # Create an instance of UserCarMap
    user_car_map = UserCarMap({"user1": [car1, car2]})
    # Convert it to a dictionary
    user_car_map_dict = user_car_map.as_json()
    # Check the result
    assert user_car_map_dict == {
        "map": {
            "user1": [
                {
                    "make": {
                        "name": "Test Manufacturer",
                        "country": "Test Country",
                    },
                    "model": "Test Model",
                    "year": 2023,
                },
                {
                    "make": {
                        "name": "Second Manufacturer",
                        "country": "Second Country",
                    },
                    "model": "Second Model",
                    "year": 2024,
                },
            ]
        }
    }

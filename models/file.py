from pydantic import BaseModel
from datetime import date


class File(BaseModel):
    first_name: str
    last_name: str
    address: str
    date_of_birth: date

    def values(self) -> dict:
        return self.__dict__

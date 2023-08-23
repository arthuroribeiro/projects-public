from faker import Faker
from pandas import DataFrame
import pandas as pd


def generate_person_dataframe(qtd_rows: int = 100, locale: str = "pt_BR") -> DataFrame:
    """
    Gera um dataframe com informações fakes de usuarios.

    Args:
        qtd_rows (int, opcional): Quantidade de linhas a serem geradas. Defaults to 100.
        locale (str, opcional): Localidade das informações geradas. Defaults to "pt_BR".

    Returns:
        DataFrame: Dataframe com informações fakes de usuarios
    """
    local_faker = Faker(locale)
    list_result = []
    for _ in range(qtd_rows):
        dict_result = {
            "name": local_faker.name(),
            "job": local_faker.job(),
            "country": local_faker.country(),
            "phone": local_faker.phone_number(),
            "email": local_faker.email(),
            "birthdate": local_faker.date(),
        }
        list_result.append(dict_result)

    return pd.DataFrame(list_result)


generate_person_dataframe(5)

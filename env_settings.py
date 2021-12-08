from os import getenv
from dotenv import load_dotenv
load_dotenv()

def get_env_vars():
    """
    Returns variables from .env as dictionary.
    """

    vars = {}
    vars["DB_USER"] = getenv("DB_USER")
    vars["DB_PASSWORD"] = getenv("DB_PASSWORD")

    return vars
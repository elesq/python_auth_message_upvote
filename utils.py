from passlib.context import CryptContext

pwd_context = CryptContext(schemes=['bcrypt'])


def get_password_hash(pwd: str):
    """ invoked by the interface the function accepts a string password
        and returns the hashed version of it."""
    return pwd_context.hash(pwd)


def verify_password(plain_pwd, hashed_pwd):
    """ accepts a hashed value and verifies this against
        the expected password """
    return pwd_context.verify(plain_pwd, hashed_pwd)

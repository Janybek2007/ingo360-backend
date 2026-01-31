from fastapi_users.password import PasswordHelper


def get_password_helper() -> PasswordHelper:
    return PasswordHelper()

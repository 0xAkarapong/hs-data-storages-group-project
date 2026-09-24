import hashlib
import os

from sqlalchemy.exc import IntegrityError

from db import BusinessError, retry_on_conflict, tx
from models import User


def _hash_password(pw: str) -> str:
    salt = os.urandom(16)
    return f"pbkdf2$100000${salt.hex()}${hashlib.pbkdf2_hmac('sha256', pw.encode(), salt, 100_000).hex()}"


@retry_on_conflict()
def register_user(email: str, password: str, display_name: str) -> int:
    """Atomic sign-up. Concurrency: the UNIQUE index on users.email is the arbiter —
    two simultaneous sign-ups with the same email cannot both win, no check-then-act race."""
    email = email.strip().lower()
    try:
        with tx() as s:
            u = User(email=email, password_hash=_hash_password(password), display_name=display_name)
            s.add(u)
            s.flush()
            return u.user_id
    except IntegrityError as exc:
        raise BusinessError(f"email already registered: {email}") from exc

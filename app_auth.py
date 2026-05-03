"""app_auth.py - Authentication module for the Streamlit UI."""
import base64
import hashlib
import hmac
import json
import os
import time
from collections.abc import Mapping

import streamlit as st

from utils import verify_password


def load_auth_users() -> dict[str, str]:
    """Load authorized users from Streamlit secrets or env var.

    Returns a dict mapping username → password-or-hash.
    """
    users: dict[str, str] = {}
    try:
        secret_users = st.secrets.get("auth_users", {})
        if isinstance(secret_users, Mapping):
            for k, v in secret_users.items():
                u = str(k).strip()
                p = str(v).strip()
                if u and p:
                    users[u] = p
    except Exception:
        pass
    env_json = os.getenv("LGD_AUTH_USERS_JSON", "").strip()
    if env_json:
        try:
            parsed = json.loads(env_json)
            if isinstance(parsed, Mapping):
                for k, v in parsed.items():
                    u = str(k).strip()
                    p = str(v).strip()
                    if u and p:
                        users[u] = p
        except Exception:
            pass
    return users


def _load_auth_token_secret() -> str:
    try:
        secret = str(st.secrets.get("auth_token_secret", "")).strip()
        if secret:
            return secret
    except Exception:
        pass
    return os.getenv("LGD_AUTH_TOKEN_SECRET", "").strip()


def _token_encode(payload: dict, secret: str) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    body = base64.urlsafe_b64encode(raw).decode("ascii")
    sig = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{body}.{sig}"


def _token_decode(token: str, secret: str) -> dict | None:
    if not token or "." not in token:
        return None
    body, sig = token.rsplit(".", 1)
    expected = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(body.encode("ascii")).decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    exp = payload.get("exp")
    user = payload.get("user")
    if not isinstance(exp, (int, float)) or not isinstance(user, str):
        return None
    if time.time() > float(exp):
        return None
    return payload


def _try_restore_auth_from_token(users: dict[str, str]) -> bool:
    if st.session_state.get("auth_ok"):
        return True
    token = st.query_params.get("auth_token")
    if not token:
        return False
    secret = _load_auth_token_secret()
    if not secret:
        return False
    payload = _token_decode(str(token), secret)
    if not payload:
        return False
    user = str(payload["user"]).strip()
    if user not in users:
        return False
    st.session_state["auth_ok"] = True
    st.session_state["auth_user"] = user
    return True


def render_auth_gate() -> None:
    """Show the login form and block execution until the user signs in."""
    users = load_auth_users()
    if _try_restore_auth_from_token(users):
        return
    if st.session_state.get("auth_ok"):
        return
    st.title("🔐 Authorized Access")
    st.caption("Sign in to use the LGD Fuzzy Matcher.")
    if not users:
        st.error(
            "No authorized users configured. Set `auth_users` in Streamlit secrets "
            "or `LGD_AUTH_USERS_JSON` in environment."
        )
        st.stop()
    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        remember_me = st.checkbox("Remember me for 24 hours")
        submit = st.form_submit_button("Sign in", type="primary")
    if submit:
        expected = users.get(str(username).strip())
        if expected and verify_password(str(password), expected):
            st.session_state["auth_ok"] = True
            st.session_state["auth_user"] = str(username).strip()
            if remember_me:
                secret = _load_auth_token_secret()
                if secret:
                    token = _token_encode(
                        {"user": str(username).strip(), "exp": int(time.time() + 24 * 3600)},
                        secret,
                    )
                    st.query_params["auth_token"] = token
                else:
                    st.info("Set `auth_token_secret` in Streamlit secrets to enable persistent login.")
            st.success("Login successful.")
            st.rerun()
        st.error("Invalid username or password.")
    st.stop()

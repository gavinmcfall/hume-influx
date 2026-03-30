"""Hume Health API client — Firebase Auth + Firestore REST."""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

FIREBASE_PROJECT = "myhealth-production"
# API key fetched at runtime from Firebase hosting config
_cached_api_key: str = ""
FIRESTORE_BASE = f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT}/databases/(default)/documents"


class HumeClient:
    """Client for Hume Health via Firebase Auth + Firestore."""

    def __init__(self, email: str, password: str):
        self._email = email
        self._password = password
        self._id_token: str = ""
        self._refresh_token: str = ""
        self._uid: str = ""

    @staticmethod
    def _get_api_key() -> str:
        """Fetch the public Firebase API key from the hosting config."""
        global _cached_api_key
        if _cached_api_key:
            return _cached_api_key
        resp = requests.get(
            f"https://{FIREBASE_PROJECT}.firebaseapp.com/__/firebase/init.json",
            timeout=10,
        )
        resp.raise_for_status()
        _cached_api_key = resp.json()["apiKey"]
        return _cached_api_key

    def login(self) -> bool:
        try:
            api_key = self._get_api_key()
            resp = requests.post(
                f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={api_key}",
                json={"email": self._email, "password": self._password, "returnSecureToken": True},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("Firebase auth failed: %d %s", resp.status_code, resp.text[:200])
                return False

            data = resp.json()
            self._id_token = data["idToken"]
            self._refresh_token = data.get("refreshToken", "")
            self._uid = data["localId"]
            logger.info("Logged in as %s (uid %s)", data.get("displayName", self._email), self._uid)
            return True
        except Exception:
            logger.exception("Login error")
            return False

    def _refresh_auth(self) -> bool:
        """Refresh the Firebase ID token."""
        if not self._refresh_token:
            return self.login()
        try:
            api_key = self._get_api_key()
            resp = requests.post(
                f"https://securetoken.googleapis.com/v1/token?key={api_key}",
                json={"grant_type": "refresh_token", "refresh_token": self._refresh_token},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                self._id_token = data["id_token"]
                self._refresh_token = data["refresh_token"]
                return True
        except Exception:
            logger.exception("Token refresh error")
        return self.login()

    def _get(self, path: str) -> dict | list | None:
        """GET from Firestore with auto-refresh."""
        if not self._id_token and not self.login():
            return None

        resp = requests.get(
            f"{FIRESTORE_BASE}{path}",
            headers={"Authorization": f"Bearer {self._id_token}"},
            timeout=15,
        )

        if resp.status_code == 401:
            if self._refresh_auth():
                resp = requests.get(
                    f"{FIRESTORE_BASE}{path}",
                    headers={"Authorization": f"Bearer {self._id_token}"},
                    timeout=15,
                )

        if resp.status_code != 200:
            logger.warning("Firestore GET %s: %d", path, resp.status_code)
            return None

        return resp.json()

    def fetch_measurements(self) -> list[dict]:
        """Fetch all body measurements from Firestore."""
        if not self._uid and not self.login():
            return []

        data = self._get(f"/users/{self._uid}/bodyMeasurements?pageSize=500&orderBy=time desc")
        if not data:
            # Try without orderBy (might not be indexed)
            data = self._get(f"/users/{self._uid}/bodyMeasurements?pageSize=500")
        if not data:
            return []

        docs = data.get("documents", [])
        measurements = []

        for doc in docs:
            fields = doc.get("fields", {})
            m = {}
            for k, v in fields.items():
                if "doubleValue" in v:
                    m[k] = v["doubleValue"]
                elif "integerValue" in v:
                    m[k] = int(v["integerValue"])
                elif "stringValue" in v:
                    m[k] = v["stringValue"]
                elif "booleanValue" in v:
                    m[k] = v["booleanValue"]

            if m.get("deleted"):
                continue
            if "weight" not in m or "deviceTime" not in m:
                continue

            measurements.append(m)

        logger.info("Fetched %d body measurements", len(measurements))
        return measurements

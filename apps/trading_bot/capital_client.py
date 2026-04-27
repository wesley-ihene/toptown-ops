"""Capital.com REST client.

Implements the subset of the v1 API needed by the bot:
session auth, account info, market metadata, historical prices,
opening / closing / updating positions.

API reference: https://open-api.capital.com/
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from .config import Credentials

log = logging.getLogger(__name__)


class CapitalAPIError(RuntimeError):
    pass


@dataclass
class Candle:
    snapshot_time: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class CapitalClient:
    def __init__(self, creds: Credentials, timeout: float = 15.0) -> None:
        self._creds = creds
        self._base = creds.base_url
        self._timeout = timeout
        self._session = requests.Session()
        self._cst: Optional[str] = None
        self._x_security: Optional[str] = None

    # ---- session ---------------------------------------------------------

    def login(self) -> Dict[str, Any]:
        url = f"{self._base}/api/v1/session"
        headers = {
            "X-CAP-API-KEY": self._creds.api_key,
            "Content-Type": "application/json",
        }
        body = {
            "identifier": self._creds.identifier,
            "password": self._creds.password,
            "encryptedPassword": False,
        }
        resp = self._session.post(url, json=body, headers=headers, timeout=self._timeout)
        if resp.status_code != 200:
            raise CapitalAPIError(f"login failed [{resp.status_code}]: {resp.text}")
        self._cst = resp.headers.get("CST")
        self._x_security = resp.headers.get("X-SECURITY-TOKEN")
        if not self._cst or not self._x_security:
            raise CapitalAPIError("login returned 200 but missing CST / X-SECURITY-TOKEN headers")
        return resp.json()

    def _auth_headers(self) -> Dict[str, str]:
        if not self._cst or not self._x_security:
            raise CapitalAPIError("not logged in -- call login() first")
        return {
            "X-SECURITY-TOKEN": self._x_security,
            "CST": self._cst,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, **kw) -> Dict[str, Any]:
        url = f"{self._base}{path}"
        headers = kw.pop("headers", {})
        headers.update(self._auth_headers())
        resp = self._session.request(method, url, headers=headers, timeout=self._timeout, **kw)
        if resp.status_code >= 400:
            raise CapitalAPIError(f"{method} {path} [{resp.status_code}]: {resp.text}")
        if not resp.content:
            return {}
        return resp.json()

    # ---- accounts --------------------------------------------------------

    def list_accounts(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/api/v1/accounts").get("accounts", [])

    def primary_account(self) -> Dict[str, Any]:
        accounts = self.list_accounts()
        if not accounts:
            raise CapitalAPIError("no accounts on this login")
        for a in accounts:
            if a.get("preferred"):
                return a
        return accounts[0]

    def account_balance(self) -> float:
        acct = self.primary_account()
        bal = acct.get("balance", {})
        return float(bal.get("balance", 0.0))

    def account_available(self) -> float:
        acct = self.primary_account()
        bal = acct.get("balance", {})
        return float(bal.get("available", 0.0))

    # ---- market data -----------------------------------------------------

    def get_market(self, epic: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v1/markets/{epic}")

    def get_prices(self, epic: str, resolution: str = "MINUTE_5", max_bars: int = 200) -> List[Candle]:
        data = self._request(
            "GET",
            f"/api/v1/prices/{epic}",
            params={"resolution": resolution, "max": max_bars},
        )
        out: List[Candle] = []
        for p in data.get("prices", []):
            # Capital.com returns bid/ask snapshots. Use mid for indicators.
            def _mid(node: Dict[str, Any]) -> float:
                bid = node.get("bid")
                ask = node.get("ask")
                if bid is None and ask is None:
                    return 0.0
                if bid is None:
                    return float(ask)
                if ask is None:
                    return float(bid)
                return (float(bid) + float(ask)) / 2.0

            out.append(Candle(
                snapshot_time=p.get("snapshotTime", ""),
                open=_mid(p.get("openPrice", {})),
                high=_mid(p.get("highPrice", {})),
                low=_mid(p.get("lowPrice", {})),
                close=_mid(p.get("closePrice", {})),
                volume=float(p.get("lastTradedVolume", 0) or 0),
            ))
        return out

    # ---- positions -------------------------------------------------------

    def list_positions(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/api/v1/positions").get("positions", [])

    def open_position(
        self,
        epic: str,
        direction: str,
        size: float,
        stop_level: Optional[float] = None,
        profit_level: Optional[float] = None,
        guaranteed_stop: bool = False,
        trailing_stop: bool = False,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "epic": epic,
            "direction": direction.upper(),
            "size": size,
            "guaranteedStop": guaranteed_stop,
            "trailingStop": trailing_stop,
        }
        if stop_level is not None:
            body["stopLevel"] = stop_level
        if profit_level is not None:
            body["profitLevel"] = profit_level
        return self._request("POST", "/api/v1/positions", json=body)

    def close_position(self, deal_id: str) -> Dict[str, Any]:
        return self._request("DELETE", f"/api/v1/positions/{deal_id}")

    def update_position(
        self,
        deal_id: str,
        stop_level: Optional[float] = None,
        profit_level: Optional[float] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if stop_level is not None:
            body["stopLevel"] = stop_level
        if profit_level is not None:
            body["profitLevel"] = profit_level
        return self._request("PUT", f"/api/v1/positions/{deal_id}", json=body)

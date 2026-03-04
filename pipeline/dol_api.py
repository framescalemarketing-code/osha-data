from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from pipeline.http_client import SafeHttpClient


@dataclass(frozen=True)
class Pagination:
    limit: int
    max_pages: int  # 0 means unbounded


class DolApiClient:
    def __init__(
        self,
        *,
        api_key: str,
        http_client: SafeHttpClient,
        base_url: str = "https://apiprod.dol.gov/v4/get/OSHA",
    ) -> None:
        self.api_key = api_key
        self.http_client = http_client
        self.base_url = base_url.rstrip("/")

    def _build_url(
        self,
        endpoint: str,
        *,
        limit: int,
        offset: int,
        sort: str,
        sort_by: str,
        filter_object: dict[str, Any] | None,
    ) -> str:
        params: dict[str, str | int] = {
            "limit": limit,
            "offset": offset,
            "sort": sort,
            "sort_by": sort_by,
            "X-API-KEY": self.api_key,
        }
        if filter_object:
            params["filter_object"] = json.dumps(
                filter_object, separators=(",", ":"), ensure_ascii=True
            )
        return f"{self.base_url}/{endpoint}/json?{urlencode(params)}"

    def get_metadata_columns(self, endpoint: str) -> list[str]:
        url = (
            f"{self.base_url}/{endpoint}/json/metadata?"
            f"{urlencode({'X-API-KEY': self.api_key})}"
        )
        payload = self.http_client.get_json(url, label=f"{endpoint} metadata")
        columns: list[str] = []
        for row in payload:
            short_name = str(row.get("short_name", "")).strip()
            if short_name and short_name not in columns:
                columns.append(short_name)
        if not columns:
            raise RuntimeError(f"[{endpoint}] metadata returned no columns.")
        return columns

    def iter_pages(
        self,
        endpoint: str,
        *,
        pagination: Pagination,
        sort: str,
        sort_by: str,
        filter_object: dict[str, Any] | None = None,
        label: str,
    ) -> list[list[dict[str, Any]]]:
        if pagination.limit < 1 or pagination.limit > 10000:
            raise ValueError("DOL API limit must be between 1 and 10000.")

        pages: list[list[dict[str, Any]]] = []
        offset = 0
        page_count = 0

        while True:
            url = self._build_url(
                endpoint,
                limit=pagination.limit,
                offset=offset,
                sort=sort,
                sort_by=sort_by,
                filter_object=filter_object,
            )
            payload = self.http_client.get_json(url, label=label)
            rows = payload.get("data", []) if isinstance(payload, dict) else []
            page_rows = [row for row in rows if isinstance(row, dict)]
            count = len(page_rows)
            if count == 0:
                break

            pages.append(page_rows)
            page_count += 1

            if count < pagination.limit:
                break
            if pagination.max_pages > 0 and page_count >= pagination.max_pages:
                break
            offset += pagination.limit

        return pages

    def fetch_rows(
        self,
        endpoint: str,
        *,
        pagination: Pagination,
        sort: str,
        sort_by: str,
        filter_object: dict[str, Any] | None = None,
        label: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in self.iter_pages(
            endpoint,
            pagination=pagination,
            sort=sort,
            sort_by=sort_by,
            filter_object=filter_object,
            label=label,
        ):
            rows.extend(page)
        return rows

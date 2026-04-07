import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from urllib.parse import urlencode, urlsplit

import aiohttp
from akamai.edgeauth import EdgeAuth

# Constants (Reuse your existing ones)
from constants import (
    API_BASE_URL,
    EDGE_AUTH_ENCRYPTION_KEY,
    HEADERS,
    TOKEN_LIFESPAN,
)


class AsyncCricInfoAuthHandler:
    """
    Async-ready Auth Handler.
    Uses asyncio.Lock() instead of threading.Lock().
    """

    def __init__(self) -> None:
        self._token_cache: Dict[str, Dict[str, Any]] = {}
        # Critical Change: Async Lock
        self._lock = asyncio.Lock()
        self._buffer_time = timedelta(seconds=10)

    def _is_token_expired(self, expiry: datetime) -> bool:
        return expiry <= datetime.now() + self._buffer_time

    async def get_auth_token(self, url: str) -> str:
        """
        Async retrieval of auth token.
        """
        # We need the lock to ensure we don't generate the same token twice simultaneously
        async with self._lock:
            query_path = urlsplit(url)._replace(scheme="", netloc="").geturl()
            cached_token = self._token_cache.get(url)

            if cached_token is None or self._is_token_expired(cached_token["expiry"]):
                expiry = datetime.now() + timedelta(seconds=TOKEN_LIFESPAN)

                # EdgeAuth is CPU-bound (fast), so we can run it directly here.
                # If it were slow, we'd run_in_executor.
                et = EdgeAuth(
                    key=EDGE_AUTH_ENCRYPTION_KEY,
                    window_seconds=TOKEN_LIFESPAN,
                    escape_early=True,
                )
                token = et.generate_url_token(query_path)

                self._token_cache[url] = {"token": token, "expiry": expiry}
                return token

            return cached_token["token"]


class AsyncCricInfoAPIClient:
    """
    Replaces your synchronous base client.
    Requires an aiohttp.ClientSession to be passed in.
    """

    def __init__(
        self, session: aiohttp.ClientSession, base_url: str = API_BASE_URL
    ) -> None:
        self.session = session
        self.base_url = base_url
        self.headers = HEADERS.copy()  # Ensure we don't mutate global constants
        self.auth_handler = AsyncCricInfoAuthHandler()

    def _prepare_query(self, query: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        return query.copy() if query else {}

    def _get_full_url(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> str:
        if self.base_url is None or endpoint.startswith(("http://", "https://")):
            url = endpoint
        else:
            url = self.base_url.rstrip("/") + "/" + endpoint.lstrip("/")

        if params:
            query_string = urlencode(params)
            url += f"?{query_string}"
        return url

    async def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
        """
        Async GET request with Auth injection.
        """
        full_url = self._get_full_url(endpoint, params)

        token = await self.auth_handler.get_auth_token(full_url)

        req_headers = self.headers.copy()
        req_headers["X-Hsci-Auth-Token"] = token
        try:
            # 2. Make the Async Request
            async with self.session.get(
                full_url, headers=req_headers, **kwargs
            ) as response:
                if response.status == 404:
                    return {}

                # Raise exception for 4xx/5xx so Tenacity can catch it
                response.raise_for_status()

                return await response.json()
        except ValueError as json_err:
            print(
                f"JSON decoding error: {json_err} - Response text: {response.text}"
            )
            raise
        except Exception as e:
            print(f"An unknown error occurred: {e}")
            traceback.print_exc()
            raise


class AsyncMatchClient(AsyncCricInfoAPIClient):
    """
    Async version of MatchClient.
    """

    def _get_query_with_ids(
        self, id: int, series_id: int = 0, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        q = self._prepare_query(query)
        q["matchId"] = id
        q["seriesId"] = series_id
        return q

    # NOTE: 'async def' and 'await' are the key changes here
    async def get_one(
        self, id: int, series_id: int = 0, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        params = self._get_query_with_ids(id, series_id, query)
        return await self.get("match/home", params)

    async def get_commentary(
        self, id: int, series_id: int = 0, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        params = self._get_query_with_ids(id, series_id, query)
        return await self.get("match/commentary", params)

    async def get_scorecard(
        self, id: int, series_id: int = 0, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        params = self._get_query_with_ids(id, series_id, query)
        return await self.get("match/scorecard", params)

    async def get_ball_commentary(
        self,
        id: int,
        series_id: int = 0,
        inning_number: int = 1,
        from_over: Optional[int] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fetch ball-by-ball commentary with pagination support."""
        params = self._get_query_with_ids(id, series_id, query)

        params.update(
            {
                "inningNumber": inning_number,
                "commentType": params.get("commentType", "ALL"),
                "sortDirection": params.get("sortDirection", "ASC"),
            }
        )

        if from_over is not None:
            params["fromInningOver"] = from_over

        return await self.get("match/comments", params)

    # NOTE: 'async def' and 'await' are the key changes here
    async def list(self, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = self._prepare_query(query)
        return await self.get("matches/result", params)


class AsyncSeriesClient(AsyncCricInfoAPIClient):
    async def get_one(
        self, id: int, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        params = self._prepare_query(query)
        params["seriesId"] = id
        return await self.get("series/home", params)

    async def get_standings(
        self, id: int, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        params = self._prepare_query(query)
        params["seriesId"] = id
        return await self.get("series/standings", params)

    async def get_schedule(
        self, id: int, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        params = self._prepare_query(query)
        params["seriesId"] = id
        print(params)
        return await self.get("series/schedule", params)


class AsyncPlayerClient(AsyncCricInfoAPIClient):
    async def get_one(
        self, id: int, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        params = self._prepare_query(query)
        params["playerId"] = id
        return await self.get("player/home", params)


class AsyncTeamClient(AsyncCricInfoAPIClient):
    async def get_one(
        self, id: int, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        params = self._prepare_query(query)
        params["teamId"] = id
        return await self.get("team/home", params)

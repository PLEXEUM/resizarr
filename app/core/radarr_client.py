import httpx
import asyncio
from typing import Optional
from datetime import datetime, timedelta
from app.utils.logger import get_logger
from app.utils.redactor import redact

logger = get_logger()

# In-memory cache for quality profiles
_quality_profiles_cache = []
_quality_profiles_last_updated: Optional[datetime] = None
CACHE_TTL_HOURS = 1


class RadarrClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.headers = {
            "X-Api-Key": api_key,
            "Content-Type": "application/json"
        }

    async def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make an HTTP request with retry logic."""
        url = f"{self.base_url}/api/v3/{endpoint}"
        last_error = None

        for attempt in range(1, 4):  # 3 attempts
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    response = await client.request(
                        method, url, headers=self.headers, **kwargs
                    )
                    response.raise_for_status()
                    return response.json()
            except httpx.HTTPStatusError as e:
                last_error = e
                logger.warning(f"Radarr API error (attempt {attempt}/3): {e.response.status_code}")
            except httpx.RequestError as e:
                last_error = e
                logger.warning(f"Radarr connection error (attempt {attempt}/3): {redact(str(e))}")

            if attempt < 3:
                wait = 2 ** attempt  # exponential backoff: 2s, 4s
                logger.info(f"Retrying in {wait} seconds...")
                await asyncio.sleep(wait)

        logger.error(f"Radarr API failed after 3 attempts: {redact(str(last_error))}")
        raise ConnectionError(f"Radarr API unreachable after 3 attempts")

    async def test_connection(self, url: str, api_key: str) -> tuple[bool, str]:
        """Test connection to a Radarr instance."""
        try:
            test_client = RadarrClient(url, api_key)
            await test_client._request("GET", "system/status")
            return True, "Connection successful"
        except Exception as e:
            return False, f"Connection failed: {redact(str(e))}"

    async def get_movies(self) -> list:
        """Fetch all movies with automatic pagination."""
        all_movies = []
        page = 1
        page_size = 50

        while True:
            logger.info(f"Fetching movies page {page}...")
            data = await self._request(
                "GET", "movie",
                params={"page": page, "pageSize": page_size}
            )

            # Radarr v5 returns a list directly, not paginated
            if isinstance(data, list):
                all_movies.extend(data)
                break

            # Handle paginated response
            records = data.get("records", [])
            all_movies.extend(records)

            total = data.get("totalRecords", 0)
            if len(all_movies) >= total or len(records) == 0:
                break

            page += 1

        logger.info(f"Fetched {len(all_movies)} movies total")
        return all_movies

    async def get_movie(self, movie_id: int) -> dict:
        """Fetch a single movie by ID."""
        return await self._request("GET", f"movie/{movie_id}")

    async def get_quality_profiles(self, force_refresh: bool = False) -> list:
        """Fetch quality profiles with 1-hour cache."""
        global _quality_profiles_cache, _quality_profiles_last_updated

        now = datetime.utcnow()
        cache_expired = (
            _quality_profiles_last_updated is None or
            now - _quality_profiles_last_updated > timedelta(hours=CACHE_TTL_HOURS)
        )

        if force_refresh or cache_expired or not _quality_profiles_cache:
            logger.info("Fetching quality profiles from Radarr...")
            _quality_profiles_cache = await self._request("GET", "qualityprofile")
            _quality_profiles_last_updated = now
            logger.info(f"Cached {len(_quality_profiles_cache)} quality profiles")

        return _quality_profiles_cache

    async def trigger_movie_search(self, movie_ids: list) -> dict:
        """Trigger a search for one or more movies."""
        logger.info(f"Triggering search for {len(movie_ids)} movie(s)")
        return await self._request("POST", "command", json={
            "name": "MoviesSearch",
            "movieIds": movie_ids
        })
    
    async def download_release_by_guid(self, guid: str) -> dict:
        """Download a specific release by GUID."""
        logger.info(f"Downloading release with GUID: {guid}")
        return await self._request("POST", "release", json={
            "guid": guid
        })

    async def download_release_by_url(self, download_url: str) -> dict:
        """Download a specific release by its download URL."""
        logger.info(f"Downloading release from URL: {download_url}")
        return await self._request("POST", "release", json={
            "downloadUrl": download_url
        })

    async def download_release_by_torrent_url(self, torrent_url: str) -> dict:
        """Download a specific release from a torrent URL."""
        logger.info(f"Downloading release from torrent URL: {torrent_url}")
        
        # Extract torrent ID from URL
        import re
        match = re.search(r'torrentid=(\d+)', torrent_url)
        if not match:
            raise ValueError(f"Could not extract torrent ID from URL: {torrent_url}")
        
        torrent_id = match.group(1)
        
        # Construct the download URL that Radarr expects
        download_url = torrent_url.replace("torrents.php?id=", "download.php?torrent=")
        
        # Try to add the release via Radarr's release endpoint
        payload = {
            "downloadClientId": 1,  # Default download client
            "downloadUrl": download_url
        }
        
        logger.info(f"Attempting to download torrent ID {torrent_id} via URL: {download_url}")
        return await self._request("POST", "release", json=payload)

    # ========== ADD THESE TWO NEW METHODS HERE ==========
    async def search_for_releases(self, movie_id: int) -> list:
        """Search for available releases for a movie."""
        try:
            logger.info(f"Searching for releases for movie ID: {movie_id}")
            result = await self._request("GET", "release", params={"movieId": movie_id})
            
            # TEMPORARY DEBUG - remove after testing
            if result and len(result) > 0:
                # Log the first release's structure to see available fields
                first_release = result[0]
                logger.info(f"DEBUG - Release keys: {list(first_release.keys())}")
                logger.info(f"DEBUG - Languages field value: {first_release.get('languages')}")
                logger.info(f"DEBUG - Languages type: {type(first_release.get('languages'))}")
                if isinstance(first_release.get('language'), dict):
                    logger.info(f"DEBUG - Language dict contents: {first_release.get('language')}")
            
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.error(f"Failed to search releases for movie {movie_id}: {e}")
            return []

    def get_release_quality_name(self, release: dict) -> str:
        """Extract quality name from a release."""
        quality = release.get("quality", {})
        return quality.get("name", "Unknown")
    # ========== END OF NEW METHODS ==========

    async def check_existing_replacement(self, movie_id: int) -> bool:
        """Check if a replacement is already queued or in history."""
        try:
            # Check queue
            queue = await self._request("GET", "queue", params={"movieId": movie_id})
            records = queue.get("records", []) if isinstance(queue, dict) else queue
            if any(r.get("movieId") == movie_id for r in records):
                return True

            # Check history for recent grabs
            history = await self._request(
                "GET", "history/movie",
                params={"movieId": movie_id, "eventType": 1}
            )
            if history:
                return True

        except Exception as e:
            logger.warning(f"Could not check existing replacement for movie {movie_id}: {e}")

        return False

    async def get_movie_quality(self, movie_id: int) -> Optional[str]:
        """Get the quality profile name of a movie's current file."""
        try:
            movie = await self.get_movie(movie_id)
            profile_id = movie.get("qualityProfileId")
            profiles = await self.get_quality_profiles()
            for profile in profiles:
                if profile.get("id") == profile_id:
                    return profile.get("name")
        except Exception as e:
            logger.warning(f"Could not get quality for movie {movie_id}: {e}")
        return None
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

    async def _request(self, method: str, endpoint: str, timeout: int = 60, **kwargs) -> dict:
        """Make an HTTP request with retry logic and full error logging."""
        url = f"{self.base_url}/api/v3/{endpoint}"
        last_error = None

        for attempt in range(1, 4):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:  # Use custom timeout
                    response = await client.request(method, url, headers=self.headers, **kwargs)
                    response.raise_for_status()
                    return response.json()
            except httpx.HTTPStatusError as e:
                last_error = e
                error_body = e.response.text if hasattr(e.response, 'text') else str(e)
                logger.warning(f"Radarr API error (attempt {attempt}/3): {e.response.status_code}")
                logger.warning(f"Error response body: {error_body[:500]}")
            except httpx.RequestError as e:
                last_error = e
                logger.warning(f"Radarr connection error (attempt {attempt}/3): {redact(str(e))}")

            if attempt < 3:
                wait = 2 ** attempt
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

            if isinstance(data, list):
                all_movies.extend(data)
                break

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

    async def delete_movie_file_only(self, movie_id: int) -> dict:
        """Delete only the movie file, keep the movie entry in Radarr."""
        try:
            movie = await self.get_movie(movie_id)
            movie_file = movie.get("movieFile")

            if not movie_file:
                logger.info(f"No movie file found for movie {movie_id}")
                return {"success": False, "message": "No file to delete"}

            file_id = movie_file.get("id")
        
            # Use _request for consistency and retry logic
            await self._request("DELETE", f"moviefile/{file_id}", timeout=120)
            logger.info(f"Deleted movie file (ID: {file_id}) for movie {movie_id}")
            return {"success": True, "message": f"Deleted file ID: {file_id}"}
        
        except Exception as e:
            logger.error(f"Failed to delete movie file: {e}")
            return {"success": False, "message": str(e)}

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

    # ─────────────────────────────────────────────────────────────
    # PRIMARY DOWNLOAD METHOD - used by both single and batch approve
    # ─────────────────────────────────────────────────────────────
    async def download_release_by_guid(
        self,
        movie_id: int,
        guid: str,
        indexerId: int = 1,
        download_url: str = None,
        title: str = None,
        publish_date: str = None
    ) -> dict:
        """Download a specific release by GUID using /release/push with all required fields."""
        payload = {
            "guid": guid,
            "indexerId": indexerId,
            "movieId": movie_id,
            "title": title or f"Release {guid}",
            "protocol": "torrent",
            "publishDate": publish_date or datetime.utcnow().isoformat(),
            "allowUpgrade": True
        }

        if download_url:
            payload["downloadUrl"] = download_url

        logger.debug(f"Push payload: {payload}")
        return await self._request("POST", "release/push", json=payload)

    async def download_release_by_url(self, movie_id: int, download_url: str, title: str = None) -> dict:
        """Download a specific release by its download URL (fallback)."""
        logger.info(f"Downloading release from URL: {download_url} for movie {movie_id}")
        payload = {
            "downloadUrl": download_url,
            "movieId": movie_id,
            "title": title or "Release from URL"
        }
        return await self._request("POST", "release", json=payload)

    async def force_grab_release(self, movie_id: int, guid: str) -> dict:
        """Force grab a release (fallback method)."""
        logger.info(f"Forcing grab of release with GUID {guid} for movie {movie_id}")
        payload = {
            "name": "ReleaseGrabbingCommand",
            "guid": guid,
            "movieId": movie_id
        }
        return await self._request("POST", "command", json=payload)

    async def download_release_by_torrent_url(self, torrent_url: str) -> dict:
        """Download from a direct torrent URL (rarely used)."""
        logger.info(f"Downloading release from torrent URL: {torrent_url}")
        import re
        match = re.search(r'torrentid=(\d+)', torrent_url)
        if not match:
            raise ValueError(f"Could not extract torrent ID from URL: {torrent_url}")
        torrent_id = match.group(1)
        download_url = torrent_url.replace("torrents.php?id=", "download.php?torrent=")
        payload = {
            "downloadClientId": 1,
            "downloadUrl": download_url
        }
        return await self._request("POST", "release", json=payload)

    async def search_for_releases(self, movie_id: int) -> list:
        """Search for available releases for a movie."""
        start_time = datetime.utcnow()
        try:
            logger.info(f"Searching for releases for movie ID: {movie_id}")
            result = await self._request("GET", "release", timeout=120, params={"movieId": movie_id})
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Search completed in {elapsed:.1f} seconds for movie {movie_id}")
            return result if isinstance(result, list) else []
        except Exception as e:
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.error(f"Failed to search releases for movie {movie_id} after {elapsed:.1f}s: {e}")
            return []

    def get_release_quality_name(self, release: dict) -> str:
        """Extract quality name from a release."""
        # The quality data is nested: release['quality']['quality']['name']
        quality_wrapper = release.get("quality", {})
        if isinstance(quality_wrapper, dict):
            quality_obj = quality_wrapper.get("quality", {})
            if isinstance(quality_obj, dict):
                quality_name = quality_obj.get("name")
                if quality_name:
                    return quality_name
    
        # Fallback: try direct quality object
        quality = release.get("quality", {})
        if isinstance(quality, dict):
            quality_name = quality.get("name")
            if quality_name:
                return quality_name
    
        # Fallback: try to infer from title
        title = release.get("title", "")
        if "1080p" in title.lower():
            return "1080p"
        elif "720p" in title.lower():
            return "720p"
        elif "4k" in title.lower() or "2160p" in title.lower():
            return "4K"
    
        return "Unknown"

    async def check_existing_replacement(self, movie_id: int) -> bool:
        """Check if a replacement is actively in Radarr's queue."""
        try:
            queue = await self._request("GET", "queue", params={"movieId": movie_id})
            records = queue.get("records", []) if isinstance(queue, dict) else queue

            for record in records:
                if record.get("movieId") == movie_id:
                    status = record.get("status", "")
                    if status in ["downloading", "queued", "paused"]:
                        logger.debug(f"Movie {movie_id} is actively in download queue (status: {status})")
                        return True

            # Check recent history
            history = await self._request(
                "GET", "history/movie",
                params={"movieId": movie_id, "eventType": 1}
            )
            if history:
                one_hour_ago = datetime.utcnow() - timedelta(hours=1)
                for item in history:
                    date_str = item.get("date")
                    if date_str and "T" in date_str:
                        item_date = date_str.split("T")[0]
                        today = datetime.utcnow().strftime("%Y-%m-%d")
                        if item_date == today:
                            logger.debug(f"Movie {movie_id} had a grab today")
                            return True
            return False
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
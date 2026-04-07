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
                # Log the response body for debugging
                error_body = e.response.text if hasattr(e.response, 'text') else str(e)
                logger.warning(f"Radarr API error (attempt {attempt}/3): {e.response.status_code}")
                logger.warning(f"Error response body: {error_body[:500]}")  # Log first 500 chars
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
    
    async def delete_movie_file_only(self, movie_id: int) -> dict:
        """Delete only the movie file, keep the movie entry in Radarr."""
        try:
            # Get the movie details to find the file ID
            movie = await self.get_movie(movie_id)
            movie_file = movie.get("movieFile")

            if not movie_file:
                logger.info(f"No movie file found for movie {movie_id}")
                return {"success": False, "message": "No file to delete"}
            
            file_id = movie_file.get("id")

            # Delete the file directly (Radarr doesn't return JSON for this endpoint)
            url = f"{self.base_url}/api/v3/moviefile/{file_id}"
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.delete(url, headers=self.headers)
                if response.status_code == 200:
                    logger.info(f"Deleted movie file (ID: {file_id}) for movie {movie_id}")
                    return {"success": True, "message": f"Deleted file ID: {file_id}"}
                else:
                    logger.warning(f"Delete returned status: {response.status_code}")
                    return {"success": False, "message": f"Status: {response.status_code}"}
        
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
    
    async def download_release_by_guid(self, movie_id: int, guid: str, indexerId: int = 1, download_url: str = None, title: str = None, publish_date: str = None) -> dict:
        """Download a specific release by GUID using the release/push endpoint with all required fields."""
        payload = {
            "guid": guid,
            "indexerId": indexerId,
            "movieId": movie_id,
            "title": title or f"Release {guid}",
            "protocol": "torrent",
            "publishDate": publish_date or datetime.utcnow().isoformat(),
            "allowUpgrade": True  # Force Radarr to allow the downgrade
        }
    
        if download_url:
            payload["downloadUrl"] = download_url
    
        logger.debug(f"Push payload: {payload}")
        return await self._request("POST", "release/push", json=payload)

    async def download_release_by_url(self, movie_id: int, download_url: str, title: str = None) -> dict:
        """Download a specific release by its download URL."""
        logger.info(f"Downloading release from URL: {download_url} for movie {movie_id}")
        
        payload = {
            "downloadUrl": download_url,
            "movieId": movie_id,
            "title": title or "Release from URL"
        }
        return await self._request("POST", "release", json=payload)
    
    async def force_grab_release(self, movie_id: int, guid: str) -> dict:
        """Force grab a release regardless of quality cutoff using the command endpoint."""
        logger.info(f"Forcing grab of release with GUID {guid} for movie {movie_id}")
    
        # Use the command endpoint which allows forced grabs regardless of cutoff
        payload = {
            "name": "ReleaseGrabbingCommand",
            "guid": guid,
            "movieId": movie_id
        }
    
        logger.debug(f"Force grab payload: {payload}")
        return await self._request("POST", "command", json=payload)

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

    async def check_existing_replacement(self, movie_id: int) -> bool:
        """Check if a replacement is actively in Radarr's queue (not just stale history)."""
        try:
            # Check active download queue first
            queue = await self._request("GET", "queue", params={"movieId": movie_id})
            records = queue.get("records", []) if isinstance(queue, dict) else queue
        
            # If it's actively in the download queue, return True
            for record in records:
                if record.get("movieId") == movie_id:
                    status = record.get("status", "")
                    # Only skip if it's actively downloading or waiting
                    if status in ["downloading", "queued", "paused"]:
                        logger.debug(f"Movie {movie_id} is actively in download queue (status: {status})")
                        return True
        
            # Check history for recent grabs (within last hour)
            history = await self._request(
                "GET", "history/movie",
                params={"movieId": movie_id, "eventType": 1}  # 1 = Grabbed
            )
        
            if history:
                from datetime import datetime, timedelta
                one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            
                for item in history:
                    date_str = item.get("date")
                    if date_str:
                        try:
                            # Parse the date (Radarr uses ISO format)
                            if isinstance(date_str, str):
                                # Simple check - if it contains today's date, consider it recent
                                if "T" in date_str:
                                    # Extract just the date part and compare
                                    item_date = date_str.split("T")[0]
                                    today = datetime.utcnow().strftime("%Y-%m-%d")
                                    if item_date == today:
                                        logger.debug(f"Movie {movie_id} had a grab today")
                                        return True
                        except Exception as e:
                            logger.debug(f"Could not parse date: {e}")
        
            return False
        
        except Exception as e:
            logger.warning(f"Could not check existing replacement for movie {movie_id}: {e}")
            return False

    async def trigger_specific_release(self, movie_id: int, guid: str, title: str = None, download_url: str = None) -> bool:
        """Trigger download of a specific release by GUID."""
        try:
            payload = {
                "guid": guid,
                "movieId": movie_id,
                "title": title or f"Release {guid}",
                "downloadUrl": download_url or "",
                "allowUpgrade": True
            }
        
            logger.info(f"Triggering specific release {guid} for movie {movie_id}")
            await self._request("POST", "release/push", json=payload)
            logger.info(f"Successfully queued release {guid} for movie {movie_id}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to trigger specific release {guid} for movie {movie_id}: {e}")
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
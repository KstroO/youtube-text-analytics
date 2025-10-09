import os
from datetime import datetime, date
from typing import Optional, List
import config

class Paths:
    def __init__(
        self,
        channel_handle: str,
        date_obj: Optional[date] = None,
        base_dir: Optional[str] = None,
    ):
        self.channel_handle = channel_handle
        self.date_obj = date_obj or date.today()
        self.date_str = self.date_obj.strftime("%Y_%m_%d")

        self.base_dir = base_dir or config.BASE_DIR

        # Base folders
        self.raw_data_dir = os.path.join(self.base_dir, "data", "raw")
        self._raw_comments_dir = os.path.join(self.raw_data_dir, "comments")
        self.processed_data_dir = os.path.join(self.base_dir, "data", "processed")
        self._clean_comments_dir = os.path.join(self.processed_data_dir, "comments")
        self._enriched_comments_dir = os.path.join(self.processed_data_dir, "enriched")
        self.results_dir = os.path.join(self.processed_data_dir, "results")

        self.resolve_all_paths(create_dirs=True)


        # Static file paths (do not change with date)
        self.videos_file_path = os.path.join(self.raw_data_dir, f"{channel_handle}_videos.json")
        self.playlists_file_path = os.path.join(self.raw_data_dir, f"{channel_handle}_playlists.json")

    # --- Raw Data Paths ---
    @property
    def raw_comments_file_path(self) -> str:
        """Return the path of the raw comments, ndjson format."""
        return os.path.join(
            self._raw_comments_dir,
            f"{self.channel_handle}_comments_{self.date_str}.ndjson"
        )

    # --- Processed Data Paths ---
    @property
    def clean_comments_file_path(self) -> str:
        """Return the path of the clean comments, parquet format."""
        return os.path.join(
            self._clean_comments_dir,
            f"{self.channel_handle}_comments_{self.date_str}.parquet"
        )
    
    @property
    def enriched_comments_file_path(self) -> str:
        """Return the path of the clean comments, parquet format."""
        return os.path.join(
            self._enriched_comments_dir,
            f"{self.channel_handle}_enriched_comments_{self.date_str}.parquet"
        )

    def as_dict(self) -> dict:
        """Return all dynamic paths as a dictionary."""
        return {
            "comments_file": self.raw_comments_file_path,
            "clean_comments_file": self.clean_comments_file_path,
        }
    
    def list_raw_files(self, show_complete_path: bool = True) -> List[str]:
        """List all raw NDJSON comment files for this channel."""
        prefix = f"{self.channel_handle}_comments_"
        suffix = ".ndjson"
        return sorted([
            os.path.join(self._raw_comments_dir, f) if show_complete_path else f
            for f in os.listdir(self._raw_comments_dir)
            if f.startswith(prefix) and f.endswith(suffix)
        ])
    
    def list_processed_files(self, show_complete_path: bool = True) -> List[str]:
        """List all processed PARQUET comment files for this channel."""
        prefix = f"{self.channel_handle}_comments_"
        suffix = ".parquet"
        return sorted([
            os.path.join(self._clean_comments_dir, f) if show_complete_path else f
            for f in os.listdir(self._clean_comments_dir)
            if f.startswith(prefix) and f.endswith(suffix)
        ])
    
    def list_processed_dates(self) -> List[str]:
        """Return a list of dates (YYYY_MM_DD) for which cleaned Parquet files exist."""
        prefix = f"{self.channel_handle}_comments_"
        suffix = ".parquet"

        dates = []
        for filename in os.listdir(self.processed_data_dir):
            if filename.startswith(prefix) and filename.endswith(suffix):
                try:
                    date_str = filename[len(prefix):-len(suffix)]
                    # Optional: validate date string format
                    datetime.strptime(date_str, "%Y_%m_%d")
                    dates.append(date_str)
                except ValueError:
                    continue  # Skip malformed filenames

        return sorted(dates)
    
    def list_enriched_files(self, show_complete_path: bool = True) -> List[str]:
        """List all enriched PARQUET comment files for this channel."""
        prefix = f"{self.channel_handle}_enriched_comments_"
        suffix = ".parquet"
        return sorted([
            os.path.join(self._enriched_comments_dir, f) if show_complete_path else f
            for f in os.listdir(self._enriched_comments_dir)
            if f.startswith(prefix) and f.endswith(suffix)
        ])
    
    def resolve_all_paths(self, create_dirs: bool = True) -> None:
        """Ensure that base data directories exist. Create them if specified."""
        for folder in [self.raw_data_dir, self.processed_data_dir, self._raw_comments_dir, self._clean_comments_dir, self._enriched_comments_dir, self.results_dir]:
            if not os.path.exists(folder):
                if create_dirs:
                    os.makedirs(folder, exist_ok=True)
                else:
                    raise FileNotFoundError(f"Required folder does not exist: {folder}")
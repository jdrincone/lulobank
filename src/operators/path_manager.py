from pathlib import Path


class PathManager:
    """Clase para gestionar rutas de directorios y archivos."""

    def __init__(self, base_dir: str = "../"):
        self.base_dir = Path(base_dir)
        self.json_dir = self.base_dir / "json"
        self.profiling_dir = self.base_dir / "profiling"
        self.parquet_dir = self.base_dir / "parquet"
        self.db_path = self.base_dir / "tv_shows.db"

        # Crear directorios si no existen
        self.json_dir.mkdir(parents=True, exist_ok=True)
        self.profiling_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)

    def get_json_path(self, date: str) -> Path:
        """Devuelve la ruta del archivo JSON para una fecha específica."""
        return self.json_dir / f"series_{date}.json"

    def get_profiling_report_path(self) -> Path:
        """Devuelve la ruta para guardar el informe de profiling."""
        return self.profiling_dir / "profiling_report.html"

    def get_parquet_path(self, name=None) -> Path:
        """Devuelve la ruta del archivo Parquet."""
        if name is not None:
            path = self.parquet_dir / f"{name}.parquet"
        else:
            path = self.parquet_dir / "tv_shows_origin.parquet"
        return path

    def get_db_path(self) -> Path:
        """Devuelve la ruta de la base de datos."""
        return self.db_path

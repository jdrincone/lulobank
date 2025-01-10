import requests
import pandas as pd
from sqlalchemy import create_engine
from pandas_profiling import ProfileReport
from src.operators.path_manager import PathManager
import logging
import json
from src.operators.models import Base


class BuildPipeline:
    """Pipeline para procesar datos de shows de TV."""

    def __init__(self, path_manager: PathManager, logger: logging.Logger):
        self.path_manager = path_manager
        self.base_url = "http://api.tvmaze.com/schedule/web?date="
        self.engine = create_engine(f"sqlite:///{self.path_manager.get_db_path()}")
        self.logger = logger

    def fetch_and_save_data(self, dates: list):
        """Obtiene datos del API y los guarda como JSON."""
        for date in dates:
            self.logger.info(f"Consultando datos para la fecha: {date}")
            response = requests.get(f"{self.base_url}{date}")
            if response.status_code == 200:
                data = response.json()
                json_path = self.path_manager.get_json_path(date)
                with open(json_path, "w") as f:
                    json.dump(data, f)
                self.logger.info(f"Datos guardados en: {json_path}")
            else:
                self.logger.error(f"Error al obtener datos para la fecha: {date}. "
                                  f"Código de estado: {response.status_code}")

    def load_data_to_dataframe(self) -> pd.DataFrame:
        """Carga datos desde los archivos JSON a un DataFrame."""
        json_files = list(self.path_manager.json_dir.glob("*.json"))
        all_data = []
        for file in json_files:
            self.logger.info(f"Cargando datos desde el archivo: {file}")
            with open(file, "r") as f:
                data = json.load(f)
                # aplana lista
                all_data.extend(data)
        self.logger.info(f"Datos cargados de {len(json_files)} archivos. Total de registros: {len(all_data)}")
        return pd.json_normalize(all_data)

    def generate_profiling_report(self, df: pd.DataFrame):
        """Genera un informe de profiling y lo guarda como HTML."""
        report_path = self.path_manager.get_profiling_report_path()
        self.logger.info(f"Generando informe de profiling en: {report_path}")
        profile = ProfileReport(df, title="TV Shows Profiling Report", explorative=True)
        profile.to_file(report_path)
        self.logger.info(f"Informe de profiling generado exitosamente.")

    def clear_dataframe(self, df: pd.DataFrame):
        self.logger.info(f"Formatiando columnas")
        df.columns = df.columns.str.replace('.', '_', regex=False)

        self.logger.info(f"Excluyendo columnas con más del 90% de valores nulos")
        null_percentage = df.isnull().mean()
        columns_with_high_nulls = null_percentage[null_percentage > 0.90].index.tolist()
        df_cleaned = df.drop(columns=columns_with_high_nulls+["summary", "type"])
        self.logger.info("Renombrando columnas")
        renames = {
            "_embedded_show_type": "type",
            "_embedded_show_language": "language",
            "_embedded_show_genres": "genres",
            "_embedded_show_status": "status",
            "_embedded_show_officialSite": "official_site",
            "_embedded_show_summary": "summary",
            "_embedded_show_rating_average": "rating_average",
            "_embedded_show_premiered": "premiered",
            "_embedded_show_ended": "ended"
        }
        df_cleaned = df_cleaned.rename(columns=renames)

        return df_cleaned

    def build_df_model_tv_show(self, df: pd.DataFrame):
        self.logger.info("Renombrando columnas")
        print(df.columns)
        cols_model = ["id", "name", "type", "language", "official_site", "runtime",
                      "premiered", "ended", "rating_average", "summary", "genres"]
        tst = df.loc[:, cols_model]

        tst_explode = tst.explode('genres')
        tst_explode = tst_explode.reset_index()
        unique_genres = tst_explode["genres"].unique()

        # Crear un DataFrame con un índice para los géneros únicos
        genres_with_index = pd.DataFrame(unique_genres, columns=["genres"])
        genres_with_index.index.name = "id"
        genres_with_index = genres_with_index.reset_index()

        tst_explode = pd.merge(tst_explode, genres_with_index, left_on="genres", right_on="genres", how='left')
        puente = tst_explode.loc[:, ["id_x", "id_y"]]
        puente = puente.drop_duplicates()
        puente.columns = ["tv_show_id", "genre_id"]
        report_path = self.path_manager.get_parquet_path(name="show_genre")
        puente.to_parquet(report_path, engine="pyarrow", compression="snappy")

        genres_with_index.columns = ["id", "name"]
        report_path = self.path_manager.get_parquet_path(name="genres")
        genres_with_index.to_parquet(report_path, engine="pyarrow", compression="snappy")

        tst = tst.drop(columns=["genres"])
        report_path = self.path_manager.get_parquet_path(name="tv_shows")
        tst.to_parquet(report_path, engine="pyarrow", compression="snappy")

    def save_to_parquet(self, df: pd.DataFrame):
        """Guarda el DataFrame en formato Parquet."""
        parquet_path = self.path_manager.get_parquet_path()
        self.logger.info(f"Almacenando datos en formato Parquet en: {parquet_path}")
        df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")
        self.logger.info(f"Datos almacenados exitosamente en: {parquet_path}")

    def save_to_database(self):
        """Guarda el DataFrame en la base de datos."""
        self.logger.info(f"Creando tablas")
        Base.metadata.create_all(self.engine)

        self.logger.info(f"Leyendo parquets")
        tv_shows_path = self.path_manager.get_parquet_path(name="tv_shows")
        tv_shows = pd.read_parquet(tv_shows_path)
        tv_shows.to_sql("tv_shows", con=self.engine, if_exists="replace", index=False)

        genres_path = self.path_manager.get_parquet_path(name="genres")
        genres = pd.read_parquet(genres_path)
        genres.to_sql("genres", con=self.engine, if_exists="replace", index=False)

        show_genre_path = self.path_manager.get_parquet_path(name="show_genre")
        show_genre = pd.read_parquet(show_genre_path)
        show_genre.to_sql("show_genre", con=self.engine, if_exists="replace", index=False)
        self.logger.info(f"Datos insertados exitosamente")

        print("Datos insertados exitosamente.")
        from sqlalchemy import text

        # Consultar la tabla desde la base de datos
        #with self.engine.connect() as conn:
        #    result = conn.execute(text("SELECT AVG(runtime) FROM tv_shows")).fetchall()
        #    for row in result:
        #        print(row)

    def perform_aggregations(self):
        """Realiza consultas de agregación en la base de datos."""

        self.logger.info("Realizando consultas de agregación.")
        query = "SELECT avg(runtime) as average_run_time FROM tv_shows"
        df_filtered = pd.read_sql(query, con=self.engine)
        self.logger.info(f"Runtime promedio: {df_filtered['average_run_time'][0]}")

        query = """SELECT g.name as genero, count(*) as cantidad_shows FROM tv_shows ts 
                    left join show_genre sg on sg.tv_show_id = ts.id
                    LEFT JOIN genres g on g.id=sg.genre_id 
                    GROUP by 1 order by 2;"""

        df_filtered = pd.read_sql(query, con=self.engine)
        self.logger.info(f"Conteo de shows de tv por género: {df_filtered}")

        query = """select DISTINCT official_site as sitios_oficiales from tv_shows ts;"""

        df_filtered = pd.read_sql(query, con=self.engine)
        self.logger.info(f"Sitios distintos oficiales: {df_filtered}")





    def perform_aggregations2(self):
        """Realiza consultas de agregación en la base de datos."""
        with self.engine.connect() as conn:
            self.logger.info("Realizando consultas de agregación.")

            # Runtime promedio
            avg_runtime = conn.execute("SELECT AVG(runtime) FROM tv_shows").scalar()
            self.logger.info(f"Runtime promedio: {avg_runtime:.2f}")

            # Conteo de shows por género
            genre_counts = conn.execute("""
                SELECT genre, COUNT(*) FROM tv_shows
                JOIN show_genre ON tv_shows.id = show_genre.tv_show_id
                GROUP BY genre
            """).fetchall()
            self.logger.info("Conteo de shows por género:")
            for genre, count in genre_counts:
                self.logger.info(f"Género: {genre}, Conteo: {count}")

            # Dominios únicos
            domains = conn.execute("""
                SELECT DISTINCT substr(official_site, instr(official_site, '.') + 1) AS domain
                FROM tv_shows
                WHERE official_site IS NOT NULL
            """).fetchall()
            self.logger.info("Dominios únicos encontrados:")
            for domain in domains:
                self.logger.info(f"Dominio: {domain[0]}")

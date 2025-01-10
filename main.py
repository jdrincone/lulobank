
from src.operators.path_manager import PathManager
from src.operators.build_pipeline import BuildPipeline
import pandas as pd
from src.logger import configure_logger
from src.operators.models import Base, engine


if __name__ == "__main__":
    logger = configure_logger("LuloBank.log")
    paths = PathManager()

    pipeline = BuildPipeline(paths, logger)

    # Fechas de enero 2024
    dates = pd.date_range("2024-01-01", "2024-01-31").strftime("%Y-%m-%d").tolist()

    # Paso 1: Obtener datos del API y guardarlos como JSON
    pipeline.fetch_and_save_data(dates)

    # Paso 2: Cargar datos en un DataFrame
    df = pipeline.load_data_to_dataframe()

    # Paso 3: Generar informe de profiling
    pipeline.generate_profiling_report(df)

    # Paso 4: Limpiar y procesar datos según sea necesario
    # (basado en el profiling)
    df_clear = pipeline.clear_dataframe(df)

    # Paso 5: Guardar en formato Parquet
    pipeline.build_df_model_tv_show(df_clear)
    pipeline.save_to_parquet(df_clear)

    # Paso 6: Guardar en la base de datos
    pipeline.save_to_database()

    # Paso 7: Realizar agregaciones
    pipeline.perform_aggregations()

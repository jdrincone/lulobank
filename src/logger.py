import logging

def configure_logger(log_file: str) -> logging.Logger:
    """Configura un logger personalizado."""
    logger = logging.getLogger("LuloBank")
    logger.setLevel(logging.INFO)

    # Formato de los mensajes
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Handler para guardar en un archivo
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Handler para mostrar en consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Agregar handlers al logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from src.operators.build_pipeline import BuildPipeline
from src.operators.path_manager import PathManager


# Mock para PathManager
@pytest.fixture
def mock_path_manager():
    return Mock(spec=PathManager)


# Fixture para BuildPipeline
@pytest.fixture
def pipeline(mock_path_manager, mock_logger):
    return BuildPipeline(mock_path_manager, mock_logger)


# Test: fetch_and_save_data
@patch("src.operators.tv_show_data_pipeline.requests.get")
def test_fetch_and_save_data(mock_get, pipeline, mock_path_manager):
    # Configurar el mock de requests
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = [{"id": 1, "name": "Test Show"}]

    # Ejecutar la función
    pipeline.fetch_and_save_data(["2024-01-01"])

    # Verificar que los datos se guardaron
    mock_path_manager.get_json_path.assert_called_once_with("2024-01-01")
    assert mock_get.called



from unittest.mock import Mock, patch, mock_open
import pytest
import json
from src.operators.build_pipeline import BuildPipeline
from src.operators.path_manager import PathManager


@pytest.fixture
def mock_path_manager():
    """Mock para PathManager."""
    mock = Mock(spec=PathManager)
    mock.get_json_path.side_effect = lambda date: f"mock_path_{date}.json"
    return mock


@pytest.fixture
def mock_logger():
    """Mock para el logger."""
    return Mock()


@pytest.fixture
def pipeline(mock_path_manager, mock_logger):
    """Instancia de BuildPipeline con mocks."""
    return BuildPipeline(mock_path_manager, mock_logger)


@patch("src.operators.build_pipeline.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_fetch_and_save_data_success(mock_open, mock_requests_get, pipeline):
    """Test para verificar que los datos se recuperan y guardan correctamente."""
    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.json.return_value = [{"id": 1, "name": "Test Show"}]



from pydantic import ValidationError
import pytest

from signaldesk_export_worker.settings import Settings


def test_package_imports_and_configuration_is_required() -> None:
    assert Settings.__name__ == "Settings"
    with pytest.raises(ValidationError):
        Settings()

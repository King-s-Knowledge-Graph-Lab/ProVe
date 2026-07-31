from pathlib import Path
import sys
import types


PACKAGE_DIR = Path(__file__).resolve().parents[1] / "src" / "prove_shared"

# Avoid executing prove_shared/__init__.py during tests; it imports runtime-heavy modules.
if "prove_shared" not in sys.modules:
    pkg = types.ModuleType("prove_shared")
    pkg.__path__ = [str(PACKAGE_DIR)]
    sys.modules["prove_shared"] = pkg

# Provide predictable test values for runtime secrets expected by shared modules.
secrets = types.ModuleType("prove_shared.local_secrets")
secrets.API_KEY = "test-api-key"
secrets.PRIVATE_KEY = str(PACKAGE_DIR / "_test_private_key.pem")
secrets.ENDPOINT = "http://localhost/"
secrets.LOG_FILENAME = "prove-shared-test.log"
secrets.LOG_PATH = str(PACKAGE_DIR / "_test_logs") + "/"
sys.modules["prove_shared.local_secrets"] = secrets

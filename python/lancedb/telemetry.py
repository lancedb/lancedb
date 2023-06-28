from posthog import Posthog
import sys
from pathlib import Path

class Telemetry:
    def __init__(self):
        self.posthog = Posthog(
            project_api_key='phc_oX2auRXmKvM9ipLuCEnIRSBCaYIlDeJEOBzAtVE8aE0',
            host='https://app.posthog.com'
        )

        # If we're in test mode, don't run telemtry
        if "pytest" in sys.modules:
            self.posthog.disabled = True

    def capture(self, event: str) -> None:
        self.posthog.capture(str(Path.home()), event)
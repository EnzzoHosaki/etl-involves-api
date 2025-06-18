import os
import base64
from dotenv import load_dotenv

load_dotenv()

INVOLVES_USERNAME = os.getenv("INVOLVES_USERNAME")
INVOLVES_PASSWORD = os.getenv("INVOLVES_PASSWORD")
INVOLVES_BASE_URL = os.getenv("INVOLVES_BASE_URL")
INVOLVES_ENVIRONMENT_ID = os.getenv("INVOLVES_ENVIRONMENT_ID")

_auth_string = f"{INVOLVES_USERNAME}:{INVOLVES_PASSWORD}"
_base64_auth_string = base64.b64encode(_auth_string.encode("utf-8")).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {_base64_auth_string}",
    "X-AGILE-CLIENT": "EXTERNAL_APP",
    "Accept-Version": "2020-02-26"
}

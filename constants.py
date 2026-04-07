USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}

API_BASE_URL = "https://hs-consumer-api.espncricinfo.com/v1/pages/"
BASE_URL = "https://www.espncricinfo.com"
IMAGE_BASE_URL = "https://p.imgci.com"
INVALID_IMAGES_URL = ["None", None, "", f"{BASE_URL}/", f"{IMAGE_BASE_URL}/"]

HSCI_TOKEN_MAP = {}
EDGE_AUTH_ENCRYPTION_KEY = "9ced54a89687e1173e91c1f225fc02abf275a119fda8a41d731d2b04dac95ff5"  # same every time for all types of requests
TOKEN_LIFESPAN = 30 * 60

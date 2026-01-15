import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os

# ---------------- CONFIG ----------------
CHANNEL_ID = "UCYn6UEtQ771a_OWSiNBoG8w"
RSS_URL = f"https://www.youtube.com/feeds/videos.xml?channel_id={CHANNEL_ID}"

SERVICE_ACCOUNT_JSON = os.environ["FIREBASE_SERVICE_ACCOUNT"]
COLLECTION_NAME = "Live-Gurdwaras-YouTube"
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]

# --------------------------------------

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015"
}

# ---------------- FIREBASE INIT ----------------
if not firebase_admin._apps:
    service_account_info = json.loads(SERVICE_ACCOUNT_JSON)
    cred = credentials.Certificate(service_account_info)
    firebase_admin.initialize_app(cred)

db = firestore.client()


def get_best_thumbnail(thumbnails: dict, video_id: str) -> str:
    for key in ("maxres", "standard", "high", "medium", "default"):
        if key in thumbnails and "url" in thumbnails[key]:
            return thumbnails[key]["url"]

    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


def fetch_video_snippet(video_id: str):
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet",
        "id": video_id,
        "key": YOUTUBE_API_KEY,
        "maxResults": 1
    }

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()

    items = r.json().get("items", [])
    return items[0] if items else None



# ---------------- RSS FETCH ----------------
def fetch_latest_hukamnama_katha():
    response = requests.get(RSS_URL, timeout=15)
    response.raise_for_status()

    root = ET.fromstring(response.text)
    matches = []

    for entry in root.findall("atom:entry", NS):
        title_el = entry.find("atom:title", NS)
        video_id_el = entry.find("yt:videoId", NS)
        published_el = entry.find("atom:published", NS)

        if title_el is None or video_id_el is None or published_el is None:
            continue

        title = title_el.text.strip()

        # ✅ FILTER: Hukamnama Katha ONLY
        if "Hukamnama Katha" not in title:
            continue

        published = datetime.fromisoformat(
            published_el.text.replace("Z", "+00:00")
        ).astimezone(timezone.utc)

        matches.append({
            "video_id": video_id_el.text.strip(),
            "title": title,
            "published": published
        })

    if not matches:
        return None

    # ✅ LATEST ONLY
    latest = max(matches, key=lambda x: x["published"])

    video_id = latest["video_id"]

    snippet_data = fetch_video_snippet(video_id)
    thumbnails = snippet_data["snippet"].get("thumbnails", {}) if snippet_data else {}

    return {
    "imageUrl": get_best_thumbnail(thumbnails, video_id),
    "title": latest["title"],
    "titleLowercase": latest["title"].lower(),
    "url": f"https://www.youtube.com/watch?v={video_id}"
    }


# ---------------- FIRESTORE UPDATE ----------------
from google.cloud.firestore_v1 import FieldFilter

def update_firestore_hukamnama(data):
    docs = (
        db.collection(COLLECTION_NAME)
        .where(filter=FieldFilter("hukamnama_katha", "==", CHANNEL_ID))
        .limit(1)
        .get()
    )

    if not docs:
        print("❌ No Firestore document found with hukamnama_katha")
        return

    doc = docs[0]
    doc_ref = doc.reference
    existing = doc.to_dict()

    # 🔒 CHANGE-DETECTION
    if existing.get("url") == data["url"]:
        print("⏭ No change detected (same Hukamnama Katha). Skipping update.")
        return

    # ✅ UPDATE ONLY IF CHANGED
    doc_ref.update({
        "imageUrl": data["imageUrl"],
        "title": data["title"],
        "titleLowercase": data["titleLowercase"],
        "url": data["url"]
    })

    print("✅ Hukamnama Katha updated successfully")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    result = fetch_latest_hukamnama_katha()

    if not result:
        print("❌ No Hukamnama Katha video found")
    else:
        print("🎯 Selected Hukamnama Katha:")
        print(result)
        update_firestore_hukamnama(result)
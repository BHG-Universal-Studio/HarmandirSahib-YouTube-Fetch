import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
from google.cloud.firestore_v1 import FieldFilter

# ---------------- CONFIG ----------------
CHANNEL_ID = "UCl2KY2TaNJ8jCwbO7CopvpA"
RSS_URL = f"https://www.youtube.com/feeds/videos.xml?channel_id={CHANNEL_ID}"

SERVICE_ACCOUNT_JSON = os.environ["FIREBASE_SERVICE_ACCOUNT"]
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]

COLLECTION_NAME = "Live-Gurdwaras-YouTube"
# --------------------------------------

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015"
}

# ---------------- FIREBASE INIT ----------------
if not firebase_admin._apps:
    cred = credentials.Certificate(json.loads(SERVICE_ACCOUNT_JSON))
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ---------------- RSS FETCH (LATEST 5 MATCHES) ----------------
def fetch_latest_5_matching():
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

        # ✅ FILTER: Live Gurbani Shabad Kirtan ONLY (UNCHANGED)
        if "Live Gurbani Shabad Kirtan" not in title:
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
        return []

    # ✅ SORT BY TIME & TAKE LATEST 5
    matches.sort(key=lambda x: x["published"], reverse=True)
    return matches[:5]

# ---------------- YOUTUBE API (SINGLE CALL) ----------------
def fetch_video_details(video_ids):
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "key": YOUTUBE_API_KEY,
        "part": "snippet,liveStreamingDetails",
        "id": ",".join(video_ids),
        "maxResults": 5
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json().get("items", [])


def get_best_thumbnail(thumbnails: dict, video_id: str) -> str:
    for key in ("maxres", "standard", "high", "medium", "default"):
        if key in thumbnails and "url" in thumbnails[key]:
            return thumbnails[key]["url"]

    # Absolute safety fallback (API should normally prevent this)
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

    
# ---------------- SELECT FINAL VIDEO ----------------
def select_best_video(rss_videos, yt_videos):
    yt_map = {v["id"]: v for v in yt_videos}

    live_candidate = None
    latest_candidate = None
    latest_time = None

    for v in rss_videos:
        yt = yt_map.get(v["video_id"])
        if not yt:
            continue

        snippet = yt["snippet"]
        live_status = snippet.get("liveBroadcastContent")

        if latest_time is None or v["published"] > latest_time:
            latest_time = v["published"]
            latest_candidate = yt

        if live_status == "live":
            live_candidate = yt
            break

    final = live_candidate if live_candidate else latest_candidate
    if not final:
        return None

    video_id = final["id"]
    thumbnails = final["snippet"].get("thumbnails", {})

    return {
    "title": final["snippet"]["title"],
    "titleLowercase": final["snippet"]["title"].lower(),
    "url": f"https://www.youtube.com/watch?v={video_id}",
    "imageUrl": get_best_thumbnail(thumbnails, video_id)
    }

# ---------------- FIRESTORE UPDATE ----------------
def update_firestore(data):
    docs = (
        db.collection(COLLECTION_NAME)
        .where(filter=FieldFilter("channel_Id", "==", CHANNEL_ID))
        .limit(1)
        .get()
    )

    if not docs:
        print("❌ No Firestore document found with channel id matching")
        return

    doc = docs[0]
    existing = doc.to_dict()

    # 🔒 CHANGE-DETECTION (UNCHANGED)
    if existing.get("url") == data["url"]:
        print("⏭ No change detected (same Live Gurbani Shabad Kirtan). Skipping update.")
        return

    doc.reference.update({
        "imageUrl": data["imageUrl"],
        "title": data["title"],
        "titleLowercase": data["titleLowercase"],
        "url": data["url"]
    })

    print("✅ Live Gurbani Shabad Kirtan updated successfully")

# ---------------- MAIN ----------------
if __name__ == "__main__":

    print("🔄 Fetching latest Live Gurbani Shabad Kirtan videos from RSS...")
    rss_videos = fetch_latest_5_matching()

    if not rss_videos:
        print("❌ No Live Gurbani Shabad Kirtan video found")
        exit(0)

    video_ids = [v["video_id"] for v in rss_videos]

    print("📡 Fetching video details from YouTube API (single call)...")
    yt_videos = fetch_video_details(video_ids)

    final_video = select_best_video(rss_videos, yt_videos)

    if not final_video:
        print("❌ No valid video selected")
        exit(0)

    print("🎯 Selected Live Gurbani Shabad Kirtan:")
    print(final_video)

    update_firestore(final_video)

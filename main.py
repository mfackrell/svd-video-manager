import os
import requests
import uuid
from functions_framework import http
from google.cloud import storage

RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]
SVD_ENDPOINT_ID = os.environ["SVD_ENDPOINT_ID"]
VIDEO_BUCKET = "ssm-video-engine-output"

SELF_URL = "https://svd-video-manager-710616455963.us-central1.run.app"

HEADERS = {
    "Authorization": f"Bearer {RUNPOD_API_KEY}",
    "Content-Type": "application/json"
}

@http
def svd_handler(request):
    data = request.get_json(silent=True) or {}

    if data.get("status") == "COMPLETED":
        video_url = data["output"]["video"]

        video = requests.get(video_url, stream=True)
        video.raise_for_status()

        client = storage.Client()
        bucket = client.bucket(VIDEO_BUCKET)

        filename = f"videos/{uuid.uuid4().hex}.mp4"
        blob = bucket.blob(filename)
        blob.upload_from_file(video.raw, content_type="video/mp4")

        return {
            "status": "complete",
            "job_id": data.get("id"),
            "gcs_url": f"https://storage.googleapis.com/{VIDEO_BUCKET}/{filename}"
        }, 200

    image_url = data.get("image_url")
    if not image_url:
        return {"error": "Missing image_url"}, 400

    payload = {
        "input": {
            "prompt": "subtle cinematic camera movement, realistic motion, shallow depth of field",
            "negative_prompt": "blurry, low quality, distorted",
            "image_url": image_url,
            "seed": 42,
            "cfg": 2.0,
            "width": 576,
            "height": 1024,
            "length": 144,
            "steps": 10
        },
        "webhook": SELF_URL
    }

    res = requests.post(
        f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run",
        headers=HEADERS,
        json=payload,
        timeout=30
    )
    res.raise_for_status()

    return {
        "status": "submitted",
        "job_id": res.json()["id"]
    }, 202

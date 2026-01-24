import os
import time
import uuid
import requests
import base64
from functions_framework import http
from google.cloud import storage

RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]
SVD_ENDPOINT_ID = os.environ["SVD_ENDPOINT_ID"]
VIDEO_BUCKET = "ssm-video-engine-output"

HEADERS = {
    "Authorization": f"Bearer {RUNPOD_API_KEY}",
    "Content-Type": "application/json"
}

POLL_INTERVAL = 5          # seconds
MAX_WAIT_SECONDS = 360     # 6 minutes


@http
def svd_video_manager(request):
    data = request.get_json(silent=True) or {}

    image_url = data.get("image_url")
    if not image_url:
        return {"error": "Missing image_url"}, 400

    # 1. SUBMIT JOB TO RUNPOD
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
        }
    }

    submit_res = requests.post(
        f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run",
        headers=HEADERS,
        json=payload,
        timeout=30
    )
    submit_res.raise_for_status()

    job_id = submit_res.json()["id"]

    # 2. POLL RUNPOD UNTIL COMPLETE
    start_time = time.time()

    while time.time() - start_time < MAX_WAIT_SECONDS:
        time.sleep(POLL_INTERVAL)

        status_res = requests.get(
            f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/status/{job_id}",
            headers=HEADERS,
            timeout=30
        )
        status_res.raise_for_status()

        status_data = status_res.json()

        if status_data["status"] == "COMPLETED":
            output = status_data.get("output", {})
            video_base64 = output.get("video")

            if not video_base64:
                return {"error": "RunPod completed but no video output"}, 500

            if video_base64.startswith("data:"):
                video_base64 = video_base64.split(",", 1)[1]

            video_bytes = base64.b64decode(video_base64)

            client = storage.Client()
            bucket = client.bucket(VIDEO_BUCKET)

            filename = f"videos/{uuid.uuid4().hex}.mp4"
            blob = bucket.blob(filename)
            blob.upload_from_string(video_bytes, content_type="video/mp4")

            return {
                "status": "complete",
                "job_id": job_id,
                "gcs_url": f"https://storage.googleapis.com/{VIDEO_BUCKET}/{filename}"
            }, 200

        if status_data["status"] == "FAILED":
            return {
                "error": "RunPod job failed",
                "details": status_data.get("error")
            }, 500

    return {
        "error": "RunPod polling timed out",
        "job_id": job_id
    }, 504



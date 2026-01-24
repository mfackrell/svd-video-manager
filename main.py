import os
import requests
import uuid
from functions_framework import http
from google.cloud import storage
import base64
import time



RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]
SVD_ENDPOINT_ID = os.environ["SVD_ENDPOINT_ID"]
VIDEO_BUCKET = "ssm-video-engine-output"

SELF_URL = "https://svd-video-manager-710616455963.us-central1.run.app"

HEADERS = {
    "Authorization": f"Bearer {RUNPOD_API_KEY}",
    "Content-Type": "application/json"
}

@http
def svd_video_manager(request):
    data = request.get_json(silent=True) or {}
    wait = data.get("wait", True)

    if data.get("status") == "COMPLETED":
        video_base64 = data["output"]["video"]
        
        # Strip data URI prefix if present
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
    }

    res = requests.post(
        f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run",
        headers=HEADERS,
        json=payload,
        timeout=30
    )
    res.raise_for_status()

    job_id = res.json()["id"]
    
    # If async behavior is explicitly requested, return immediately
    if not wait:
        return {
            "status": "submitted",
            "job_id": job_id
        }, 202
    
    # BLOCKING MODE — poll RunPod until completion
    while True:
        status_res = requests.get(
            f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/status/{job_id}",
            headers=HEADERS,
            timeout=15
        )
        status_res.raise_for_status()
        status_json = status_res.json()
    
        if status_json.get("status") == "COMPLETED":
            # Re-enter the same handler with the completed payload
            return svd_video_manager(
                type(
                    "Request",
                    (),
                    {"get_json": lambda self, silent=True: status_json}
                )()
            )
    
        if status_json.get("status") == "FAILED":
            return {
                "status": "failed",
                "job_id": job_id,
                "error": status_json
            }, 500
    
        time.sleep(5)


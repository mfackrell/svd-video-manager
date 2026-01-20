import os
import requests
from functions_framework import http

RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY")
WAN22_ENDPOINT_ID = os.environ.get("SVD_ENDPOINT_ID")  # reuse env var for now

@http
def svd_video_manager(request):
    body = request.get_json(silent=True) or {}
    image_url = body.get("image_url")

    if not image_url:
        return {"status": "error", "message": "Missing image_url"}, 400

    headers = {
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "input": {
            "prompt": "subtle cinematic camera movement, realistic motion, shallow depth of field",
            "negative_prompt": "blurry, low quality, distorted",
            "image_url": image_url,
            "seed": 42,
            "cfg": 2.0,
            "width": 576,
            "height": 1024,
            "length": 36,   # ~3s @ 12fps (fast test)
            "steps": 10
        }
    }

    try:
        submit_url = f"https://api.runpod.ai/v2/{WAN22_ENDPOINT_ID}/run"
        res = requests.post(submit_url, headers=headers, json=payload, timeout=30)
        res.raise_for_status()

        job_id = res.json().get("id")

        return {
            "status": "submitted",
            "job_id": job_id,
            "status_url": f"https://api.runpod.ai/v2/{WAN22_ENDPOINT_ID}/status/{job_id}"
        }, 202

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

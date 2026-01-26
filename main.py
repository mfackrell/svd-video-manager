import os
import requests
import uuid
import base64
import json
import tempfile
import subprocess
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
def svd_video_manager(request):
    data = request.get_json(silent=True) or {}

    # ======================================================
    # 1. WEBHOOK CALLBACK FROM RUNPOD (COMPLETED)
    # ======================================================
    if data.get("status") == "COMPLETED":
        video_base64 = data.get("output", {}).get("video")

        if not video_base64:
            return {"error": "Completed job but no video output"}, 500

        if video_base64.startswith("data:"):
            video_base64 = video_base64.split(",", 1)[1]

        video_bytes = base64.b64decode(video_base64)
        with tempfile.TemporaryDirectory() as tmp:
            in_path = os.path.join(tmp, "input.mp4")
            out_path = os.path.join(tmp, "output.mp4")
        
            # write original video
            with open(in_path, "wb") as f:
                f.write(video_bytes)
        
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i", in_path,
                    "-filter:v", "setpts=2.0*PTS",
                    "-an",
                    out_path
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # If ffmpeg failed, fall back to original video
            if result.returncode != 0:
                # OPTIONAL: log stderr for debugging
                print("FFmpeg failed:", result.stderr.decode("utf-8"), flush=True)
                out_path = in_path


        client = storage.Client()
        bucket = client.bucket(VIDEO_BUCKET)

        filename = f"videos/{uuid.uuid4().hex}.mp4"
        blob = bucket.blob(filename)
        blob.upload_from_filename(out_path, content_type="video/mp4")

        video_url = f"https://storage.googleapis.com/{VIDEO_BUCKET}/{filename}"
        
        # 🔴 ADD THIS BLOCK EXACTLY HERE
        job_blob = bucket.blob(f"jobs/{data.get('id')}.json")
        job_blob.upload_from_string(
            json.dumps({
                "status": "complete",
                "videoUrl": video_url
            }),
            content_type="application/json"
        )
        # 🔴 END ADDITION

        return {
            "status": "complete",
            "job_id": data.get("id"),
            "gcs_url": f"https://storage.googleapis.com/{VIDEO_BUCKET}/{filename}"
        }, 200

    # ======================================================
    # 2. INITIAL REQUEST (SUBMIT JOB)
    # ======================================================
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
            "length": 32,
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

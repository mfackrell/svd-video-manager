import os
import requests
import uuid
import base64
import json
import tempfile
import subprocess
from functions_framework import http
from google.cloud import storage

CHUNK_FRAMES = 36
TOTAL_LOOPS = 4
OUTPUT_FPS = 12

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
        root_id = request.args.get("root_id")
        if not root_id:
            return {"error": "Missing root_id"}, 400
    
        video_base64 = data.get("output", {}).get("video")
        if not video_base64:
            return {"error": "Completed job but no video output"}, 500
    
        if video_base64.startswith("data:"):
            video_base64 = video_base64.split(",", 1)[1]
    
        video_bytes = base64.b64decode(video_base64)
    
        client = storage.Client()
        bucket = client.bucket(VIDEO_BUCKET)
    
        job_blob = bucket.blob(f"jobs/{root_id}.json")
        if not job_blob.exists():
            return {"error": "Job state missing"}, 500
    
        job = json.loads(job_blob.download_as_text())
    
        loop = job["loop"]
    
        # Save chunk
        chunk_path = f"videos/{root_id}/chunk_{loop}.mp4"
        bucket.blob(chunk_path).upload_from_string(
            video_bytes, content_type="video/mp4"
        )
    
        job["chunks"].append(chunk_path)
        job["loop"] += 1
    
        # 🔴 STOP CONDITION
        if job["loop"] >= TOTAL_LOOPS:
            job["status"] = "svd_complete"
            job_blob.upload_from_string(
                json.dumps(job),
                content_type="application/json"
            )
    
            return {
                "status": "svd_complete",
                "root_id": root_id,
                "chunks": job["chunks"]
            }, 200
    
        # 🔁 SUBMIT NEXT SVD LOOP (SAME IMAGE)
        payload = {
            "input": {
                "prompt": "subtle cinematic camera movement, realistic motion, shallow depth of field",
                "negative_prompt": "blurry, low quality, distorted",
                "image_url": job["image_url"],
                "seed": 42,
                "cfg": 2.0,
                "width": 576,
                "height": 1024,
                "length": CHUNK_FRAMES,
                "steps": 10
            },
            "webhook": f"{SELF_URL}?root_id={root_id}"
        }
    
        requests.post(
            f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run",
            headers=HEADERS,
            json=payload,
            timeout=30
        ).raise_for_status()
    
        job_blob.upload_from_string(
            json.dumps(job),
            content_type="application/json"
        )
    
        return {
            "status": "continuing",
            "root_id": root_id,
            "loop": job["loop"]
        }, 202

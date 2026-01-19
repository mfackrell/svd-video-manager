import os
import time
import requests
import uuid
from functions_framework import http
from google.cloud import storage

# Required Environment Variables
RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY")
SVD_ENDPOINT_ID = os.environ.get("SVD_ENDPOINT_ID")
VIDEO_BUCKET = "ssm-video-engine-output"

@http
def svd_video_manager(request):
    request_json = request.get_json(silent=True) or {}
    image_url = request_json.get("image_url")
    
    if not image_url:
        return {"status": "error", "message": "Missing image_url"}, 400

    headers = {
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
        "Content-Type": "application/json"
    }

    # Configuration for a 12-second video
    payload = {
        "input": {
            "image_url": image_url,
            "prompt": "",  # ADD THIS LINE
            "motion_bucket_id": 127,
            "fps": 12,
            "num_frames": 144  # 12 seconds at 12 fps
        }
    }

    try:
        # 1. Submit SVD Job
        submit_url = f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run"
        submit_res = requests.post(submit_url, headers=headers, json=payload, timeout=30)
        submit_res.raise_for_status()
        job_id = submit_res.json().get("id")

        # 2. Poll for Completion (Video takes ~2-4 minutes)
        status_url = f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/status/{job_id}"
        video_url = None

        for _ in range(150): # Poll for up to 5 minutes
            status_res = requests.get(status_url, headers=headers, timeout=10).json()
            job_status = status_res.get("status")

            if job_status == "COMPLETED":
                output = status_res.get("output")
                # Normalize: handles string URL or dict with 'video' key
                video_url = output if isinstance(output, str) else output.get("video")
                break
            
            if job_status == "FAILED":
                return {"status": "error", "message": "SVD job failed", "details": status_res}, 500
            
            time.sleep(2)

        if not video_url:
            return {"status": "error", "message": "Video generation timed out"}, 504

        # 3. Stream from RunPod to GCS
        # Using stream=True to handle large video files efficiently
        video_response = requests.get(video_url, stream=True)
        video_response.raise_for_status()
        
        client = storage.Client()
        bucket = client.bucket(VIDEO_BUCKET)
        
        filename = f"videos/{uuid.uuid4().hex[:8]}_svd.mp4"
        blob = bucket.blob(filename)
        blob.upload_from_string(video_response.content, content_type="video/mp4")

        return {
            "status": "success",
            "video_url": f"https://storage.googleapis.com/{VIDEO_BUCKET}/{filename}",
            "gcs_path": f"gs://{VIDEO_BUCKET}/{filename}",
            "job_id": job_id
        }, 200

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

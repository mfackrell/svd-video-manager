import os
import time
import uuid
import base64
import requests
from functions_framework import http
from google.cloud import storage

# ======================
# CONFIG
# ======================

RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]
SVD_ENDPOINT_ID = os.environ["SVD_ENDPOINT_ID"]
VIDEO_BUCKET = "ssm-video-engine-output"

HEADERS = {
    "Authorization": f"Bearer {RUNPOD_API_KEY}",
    "Content-Type": "application/json"
}

# VIDEO SETTINGS
FPS = 12
TOTAL_SECONDS = 12
TOTAL_FRAMES = FPS * TOTAL_SECONDS      # 144
CHUNK_FRAMES = 32                       # HARD CAP
POLL_INTERVAL = 10                      # seconds
MAX_WAIT_SECONDS = 600                  # 10 minutes per chunk

# ======================
# HANDLER
# ======================

@http
def svd_video_manager(request):
    data = request.get_json(silent=True) or {}

    image_url = data.get("image_url")
    if not image_url:
        return {"error": "Missing image_url"}, 400

    client = storage.Client()
    bucket = client.bucket(VIDEO_BUCKET)

    current_image_url = image_url
    remaining_frames = TOTAL_FRAMES
    chunk_files = []

    # ======================
    # CHUNK LOOP
    # ======================

    while remaining_frames > 0:
        frames_this_chunk = min(CHUNK_FRAMES, remaining_frames)

        payload = {
            "input": {
                "prompt": "subtle cinematic camera movement, realistic motion, shallow depth of field",
                "negative_prompt": "blurry, low quality, distorted",
                "image_url": current_image_url,
                "seed": 42,
                "cfg": 2.0,
                "steps": 10,
                "width": 576,
                "height": 1024,
                "length": frames_this_chunk
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

        start_time = time.time()
        video_bytes = None

        # ======================
        # POLL RUNPOD
        # ======================

        while time.time() - start_time < MAX_WAIT_SECONDS:
            time.sleep(POLL_INTERVAL)

            status_res = requests.get(
                f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/status/{job_id}",
                headers=HEADERS,
                timeout=30
            )
            status_res.raise_for_status()

            status = status_res.json()

            if status["status"] == "COMPLETED":
                video_base64 = status.get("output", {}).get("video")
                if not video_base64:
                    return {"error": "RunPod completed with no video output"}, 500

                if video_base64.startswith("data:"):
                    video_base64 = video_base64.split(",", 1)[1]

                video_bytes = base64.b64decode(video_base64)
                break

            if status["status"] == "FAILED":
                return {
                    "error": "RunPod job failed",
                    "details": status.get("error")
                }, 500

        if video_bytes is None:
            return {
                "error": "RunPod polling timed out",
                "job_id": job_id
            }, 504

        # ======================
        # SAVE CHUNK
        # ======================

        chunk_path = f"/tmp/chunk_{len(chunk_files)}.mp4"
        with open(chunk_path, "wb") as f:
            f.write(video_bytes)

        chunk_files.append(chunk_path)

        # ======================
        # EXTRACT LAST FRAME
        # ======================

        last_frame_path = f"/tmp/last_frame_{len(chunk_files)}.png"
        os.system(
            f"ffmpeg -y -sseof -1 -i {chunk_path} -vframes 1 {last_frame_path}"
        )

        frame_blob = bucket.blob(f"frames/{uuid.uuid4().hex}.png")
        frame_blob.upload_from_filename(last_frame_path)
        current_image_url = f"https://storage.googleapis.com/{VIDEO_BUCKET}/{frame_blob.name}"

        remaining_frames -= frames_this_chunk

    # ======================
    # STITCH FINAL VIDEO @ 12 FPS
    # ======================

    concat_file = "/tmp/concat.txt"
    with open(concat_file, "w") as f:
        for path in chunk_files:
            f.write(f"file '{path}'\n")

    final_path = f"/tmp/final_{uuid.uuid4().hex}.mp4"
    os.system(
        f"ffmpeg -y -f concat -safe 0 -i {concat_file} -r 12 -c:v libx264 {final_path}"
    )

    final_blob = bucket.blob(f"videos/{uuid.uuid4().hex}.mp4")
    final_blob.upload_from_filename(final_path, content_type="video/mp4")

    return {
        "status": "complete",
        "gcs_url": f"https://storage.googleapis.com/{VIDEO_BUCKET}/{final_blob.name}"
    }, 200


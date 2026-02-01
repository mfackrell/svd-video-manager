# svd-video-manager/main.py

import os
import requests
import uuid
import base64
import json
import tempfile
import time
import subprocess

from functions_framework import http
from google.cloud import storage

print(subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True).stdout)

CHUNK_FRAMES = 30
TOTAL_LOOPS = 3
VIDEO_BUCKET = "ssm-video-engine-output"

SVD_ENDPOINT_ID = os.environ.get("SVD_ENDPOINT_ID")
RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY")
SELF_URL = "https://svd-video-manager-710616455963.us-central1.run.app"


def extract_last_frame_png(video_bytes):
    with tempfile.TemporaryDirectory() as tmp:
        in_path = os.path.join(tmp, "chunk.mp4")
        out_path = os.path.join(tmp, "last.png")

        with open(in_path, "wb") as f:
            f.write(video_bytes)

        subprocess.run(
            ["ffmpeg", "-y", "-sseof", "-1", "-i", in_path, "-frames:v", "1", out_path],
            check=True
        )

        with open(out_path, "rb") as f:
            return f.read()


def stitch_chunks_to_final(bucket, root_id, chunk_paths):
    with tempfile.TemporaryDirectory() as tmp:
        local_paths = []

        for i, chunk_path in enumerate(chunk_paths):
            local_path = os.path.join(tmp, f"chunk_{i}.mp4")
            bucket.blob(chunk_path).download_to_filename(local_path)
            local_paths.append(local_path)

        list_path = os.path.join(tmp, "list.txt")
        with open(list_path, "w", encoding="utf-8") as f:
            for p in local_paths:
                f.write(f"file '{p}'\n")

        # --- stage 1: concat (unchanged behavior)
        concat_path = os.path.join(tmp, "concat.mp4")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_path, "-c", "copy", concat_path],
            check=True
        )
        
        # --- stage 2: REAL final render (this is the missing piece)
        final_render_path = os.path.join(tmp, "final.mp4")
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", concat_path,
                "-vf", "fps=30",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                final_render_path
            ],
            check=True
        )
        
        final_path = f"videos/{root_id}/final.mp4"
        bucket.blob(final_path).upload_from_filename(
            final_render_path,
            content_type="video/mp4"
        )


        return f"https://storage.googleapis.com/{VIDEO_BUCKET}/{final_path}"


def start_svd_base_video(data, bucket):
    image_url = data["image_url"]

    root_id = uuid.uuid4().hex
    
    job = {
        "status": "PENDING",
        "root_id": root_id,
        "started_at": time.time(),
        "current_image_url": image_url,
        "loop": 0,
        "chunks": []
    }

    bucket.blob(f"jobs/{root_id}.json").upload_from_string(json.dumps(job))

    SVD_NEGATIVE_PROMPT = (
        "people, person, human, humans, face, faces, body, bodies, "
        "silhouette, character, characters, man, woman, child, "
        "hands, arms, legs"
    )

    SVD_PROMPT = (
        "abstract cinematic background motion, environmental movement, "
        "atmospheric depth, natural motion, no characters"
    )

    payload = {
        "input": {
            "image_url": image_url,
            "steps": 10,
            "prompt": SVD_PROMPT,
            "negative_prompt": SVD_NEGATIVE_PROMPT
        },
        "webhook": f"{SELF_URL}?root_id={root_id}"
    }

    requests.post(
        f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run",
        headers={
            "Authorization": f"Bearer {RUNPOD_API_KEY}",
            "Content-Type": "application/json"
        },
        json=payload,
        timeout=30
    )

    return {"state": "PENDING", "jobId": root_id}, 202


@http
def svd_video_manager(request):
    print(f"Content-Type: {request.content_type}")
    print(f"Raw body: {request.get_data()}")
    data = request.get_json(silent=True) or {}
    print(f"Parsed data: {data}")

    if not SVD_ENDPOINT_ID or not RUNPOD_API_KEY:
        return {"error": "Missing required environment variables"}, 500

    client = storage.Client()
    bucket = client.bucket(VIDEO_BUCKET)

    # ---- RUNPOD FAILURE CALLBACK
    if data.get("status") == "FAILED":
        root_id = request.args.get("root_id")
        if not root_id:
            return {"error": "missing root_id"}, 400
    
        job_blob = bucket.blob(f"jobs/{root_id}.json")
        job = json.loads(job_blob.download_as_text())

        # ---- HARD STOP: job already completed (idempotency guard)
        if job.get("status") == "COMPLETE":
            return {
                "status": "COMPLETE",
                "final_video_url": job.get("final_video_url")
            }, 200            
    
        job["status"] = "FAILED"
        job["error"] = data.get("error")
        job["failed_at"] = time.time()
    
        job_blob.upload_from_string(json.dumps(job))
    
        # IMPORTANT: return 200 so RunPod stops retrying
        return {
            "status": "failed",
            "error": data.get("error")
        }, 200

    
    # ---- RUNPOD CALLBACK
    if data.get("status") == "COMPLETED" or "output" in data:
        root_id = request.args.get("root_id")
        if not root_id:
            return {"error": "missing root_id"}, 400

        job_blob = bucket.blob(f"jobs/{root_id}.json")
        job = json.loads(job_blob.download_as_text())
        
        # ---- HARD STOP: job already completed (idempotency guard)
        if job.get("status") == "COMPLETE":
            return {
                "status": "COMPLETE",
                "final_video_url": job.get("final_video_url")
            }, 200

        video_b64 = data["output"]["video"]

        if video_b64.startswith("data:"):
            video_b64 = video_b64.split(",", 1)[1]

        video_bytes = base64.b64decode(video_b64)

        loop = job["loop"]

        chunk_path = f"videos/{root_id}/chunk_{loop}.mp4"
        bucket.blob(chunk_path).upload_from_string(video_bytes, content_type="video/mp4")
        job["chunks"].append(chunk_path)

        last_frame_bytes = extract_last_frame_png(video_bytes)
        frame_path = f"images/{root_id}/last_frame_{loop}.png"
        bucket.blob(frame_path).upload_from_string(last_frame_bytes, content_type="image/png")

        job["current_image_url"] = f"https://storage.googleapis.com/{VIDEO_BUCKET}/{frame_path}"
        job["loop"] = loop + 1

        if job["loop"] >= TOTAL_LOOPS:
            # mark finalization phase BEFORE heavy FFmpeg
            job["status"] = "FINALIZING"
            job_blob.upload_from_string(json.dumps(job))
        
            final_url = stitch_chunks_to_final(
                bucket, root_id, job["chunks"]
            )
        
            job["status"] = "COMPLETE"
            job["final_video_url"] = final_url
            job_blob.upload_from_string(json.dumps(job))
        
            return {
                "status": "COMPLETE",
                "final_video_url": final_url
            }, 200

        payload = {
            "input": {"image_url": job["current_image_url"], "steps": 10},
            "webhook": f"{SELF_URL}?root_id={root_id}"
        }

        requests.post(
            f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run",
            headers={
                "Authorization": f"Bearer {RUNPOD_API_KEY}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=30
        )

        # only persist looping state if job is NOT complete
        if job.get("status") != "COMPLETE":
            job_blob.upload_from_string(json.dumps(job))
        
        return {"status": "looping", "loop": job["loop"]}, 200


    # ---- INITIAL BASE VIDEO REQUEST
    if "image_url" in data:
        return start_svd_base_video(data, bucket)

    return {
        "error": "Invalid payload",
        "received_content_type": request.content_type,
        "received_data": data,
        "hint": "Expected 'image_url' for new job or 'status'='COMPLETED' for callback"
    }, 400

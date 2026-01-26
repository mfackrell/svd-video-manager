import os
import uuid
import json
import base64
import tempfile
import subprocess
import requests
import shutil

from functions_framework import http
from google.cloud import storage

FPS = 12
TOTAL_SECONDS = 12
TOTAL_FRAMES = FPS * TOTAL_SECONDS
CHUNK_FRAMES = 32

RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]
SVD_ENDPOINT_ID = os.environ["SVD_ENDPOINT_ID"]
VIDEO_BUCKET = "ssm-video-engine-output"

SELF_URL = "https://svd-video-manager-710616455963.us-central1.run.app"

HEADERS = {
    "Authorization": f"Bearer {RUNPOD_API_KEY}",
    "Content-Type": "application/json"
}

def _gcs():
    client = storage.Client()
    bucket = client.bucket(VIDEO_BUCKET)
    return client, bucket

def _public_url(path: str) -> str:
    return f"https://storage.googleapis.com/{VIDEO_BUCKET}/{path}"

print("ffmpeg path:", shutil.which("ffmpeg"), flush=True)

def _ffmpeg_last_frame(video_path: str, frame_path: str):
    r = subprocess.run(
        ["ffmpeg", "-y", "-sseof", "-1", "-i", video_path, "-vframes", "1", frame_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-2000:] if r.stderr else "ffmpeg failed")

def _ffmpeg_concat(local_paths, out_path: str):
    concat_txt = out_path + ".txt"
    with open(concat_txt, "w") as f:
        for p in local_paths:
            f.write(f"file '{p}'\n")
    r = subprocess.run(
        ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", concat_txt, "-r", str(FPS), "-c:v", "libx264", out_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-2000:] if r.stderr else "ffmpeg concat failed")

@http
def svd_video_manager(request):
    data = request.get_json(silent=True) or {}
    root_id = request.args.get("root_id")

    client, bucket = _gcs()

    if data.get("status") == "COMPLETED":
        if not root_id:
            return {"error": "Missing root_id in webhook URL"}, 400

        job_blob = bucket.blob(f"jobs/{root_id}.json")
        if not job_blob.exists():
            return {"error": f"Missing job state for root_id={root_id}"}, 500

        job_state = json.loads(job_blob.download_as_text())

        runpod_job_id = data.get("id")
        output = data.get("output") or {}
        video_base64 = output.get("video")

        if not video_base64:
            return {"error": "Completed job but no video output"}, 500

        if video_base64.startswith("data:"):
            video_base64 = video_base64.split(",", 1)[1]

        expected_chunk_index = int(job_state.get("expected_chunk_index", 0))
        last_completed_runpod_job_id = job_state.get("last_completed_runpod_job_id")

        if last_completed_runpod_job_id == runpod_job_id:
            return {"status": "duplicate_webhook_ignored", "root_id": root_id}, 200

        chunks = job_state.get("chunks", [])
        if len(chunks) != expected_chunk_index:
            return {"status": "out_of_order_webhook_ignored", "root_id": root_id}, 200

        video_bytes = base64.b64decode(video_base64)

        chunk_index = expected_chunk_index
        chunk_gcs_path = f"videos/{root_id}/chunk_{chunk_index}.mp4"
        bucket.blob(chunk_gcs_path).upload_from_string(video_bytes, content_type="video/mp4")
        chunks.append(chunk_gcs_path)

        local_video = f"/tmp/chunk_{root_id}_{chunk_index}.mp4"
        local_frame = f"/tmp/last_frame_{root_id}_{chunk_index}.png"
        with open(local_video, "wb") as f:
            f.write(video_bytes)

        try:
            _ffmpeg_last_frame(local_video, local_frame)
        except Exception as e:
            return {"error": "ffmpeg failed extracting last frame", "detail": str(e)}, 500

        frame_gcs_path = f"frames/{root_id}/frame_{chunk_index}.png"
        bucket.blob(frame_gcs_path).upload_from_filename(local_frame, content_type="image/png")
        last_frame_url = _public_url(frame_gcs_path)

        frames_done = int(job_state.get("frames_done", 0)) + int(job_state.get("chunk_frames", CHUNK_FRAMES))

        if frames_done >= TOTAL_FRAMES:
            local_chunk_paths = []
            for i, c in enumerate(chunks):
                lp = f"/tmp/concat_{root_id}_{i}.mp4"
                r = requests.get(_public_url(c), timeout=60)
                r.raise_for_status()
                with open(lp, "wb") as f:
                    f.write(r.content)
                local_chunk_paths.append(lp)

            final_local = f"/tmp/final_{root_id}.mp4"
            try:
                _ffmpeg_concat(local_chunk_paths, final_local)
            except Exception as e:
                return {"error": "ffmpeg failed stitching final video", "detail": str(e)}, 500

            final_gcs_path = f"videos/{root_id}/final.mp4"
            bucket.blob(final_gcs_path).upload_from_filename(final_local, content_type="video/mp4")
            final_url = _public_url(final_gcs_path)

            job_blob.upload_from_string(json.dumps({
                "status": "complete",
                "root_id": root_id,
                "frames_done": frames_done,
                "chunks": chunks,
                "expected_chunk_index": chunk_index + 1,
                "last_frame_url": last_frame_url,
                "last_completed_runpod_job_id": runpod_job_id,
                "videoUrl": final_url
            }), content_type="application/json")

            return {"status": "complete", "root_id": root_id, "gcs_url": final_url}, 200

        job_state.update({
            "status": "processing",
            "root_id": root_id,
            "frames_done": frames_done,
            "chunks": chunks,
            "last_frame_url": last_frame_url,
            "expected_chunk_index": chunk_index + 1,
            "last_completed_runpod_job_id": runpod_job_id
        })
        job_blob.upload_from_string(json.dumps(job_state), content_type="application/json")

        next_payload = {
            "input": {
                "prompt": job_state["prompt"],
                "negative_prompt": job_state["negative_prompt"],
                "image_url": last_frame_url,
                "seed": int(job_state["seed"]),
                "cfg": float(job_state["cfg"]),
                "width": int(job_state["width"]),
                "height": int(job_state["height"]),
                "length": int(job_state["chunk_frames"]),
                "steps": int(job_state["steps"])
            },
            "webhook": f"{SELF_URL}?root_id={root_id}"
        }

        r = requests.post(
            f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run",
            headers=HEADERS,
            json=next_payload,
            timeout=30
        )
        r.raise_for_status()

        return {"status": "continuing", "root_id": root_id}, 202

    image_url = data.get("image_url")
    if not image_url:
        return {"error": "Missing image_url"}, 400

    root_id = str(uuid.uuid4())

    job_state = {
        "status": "processing",
        "root_id": root_id,
        "frames_done": 0,
        "chunks": [],
        "expected_chunk_index": 0,
        "last_frame_url": image_url,
        "chunk_frames": CHUNK_FRAMES,
        "prompt": "subtle cinematic camera movement, realistic motion, shallow depth of field",
        "negative_prompt": "blurry, low quality, distorted",
        "seed": 42,
        "cfg": 2.0,
        "width": 576,
        "height": 1024,
        "steps": 10
    }

    bucket.blob(f"jobs/{root_id}.json").upload_from_string(json.dumps(job_state), content_type="application/json")

    payload = {
        "input": {
            "prompt": job_state["prompt"],
            "negative_prompt": job_state["negative_prompt"],
            "image_url": image_url,
            "seed": job_state["seed"],
            "cfg": job_state["cfg"],
            "width": job_state["width"],
            "height": job_state["height"],
            "length": job_state["chunk_frames"],
            "steps": job_state["steps"]
        },
        "webhook": f"{SELF_URL}?root_id={root_id}"
    }

    res = requests.post(
        f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run",
        headers=HEADERS,
        json=payload,
        timeout=30
    )
    res.raise_for_status()

    return {"status": "submitted", "root_id": root_id, "runpod_job_id": res.json().get("id")}, 202

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

VIDEO_BUCKET = "ssm-video-engine-output"

SVD_ENDPOINT_ID = os.environ.get("SVD_ENDPOINT_ID")
RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY")
SELF_URL = "https://svd-video-manager-710616455963.us-central1.run.app"
SVD_NEGATIVE_PROMPT = (
    "people, person, human, humans, face, faces, body, bodies, "
    "silhouette, character, characters, man, woman, child, "
    "hands, arms, legs"
)

SVD_PROMPT = (
    "cinematic background motion, environmental movement, "
    "atmospheric depth, natural motion, no characters"
)

SLOW_MO_FACTOR = float(os.environ.get("SLOW_MO_FACTOR", "3.0"))
SLOW_MO_OUTPUT_FPS = int(os.environ.get("SLOW_MO_OUTPUT_FPS", "15"))
SLOW_MO_INTERPOLATION_MODE = os.environ.get("SLOW_MO_INTERPOLATION_MODE", "mci")


def download_to_tempfile(bucket, remote_path, tmp_dir):
    local_path = os.path.join(tmp_dir, os.path.basename(remote_path))
    bucket.blob(remote_path).download_to_filename(local_path)
    return local_path


def download_optional_asset(bucket, tmp_dir, *, bucket_path=None, remote_url=None, filename=None):
    if bucket_path and not str(bucket_path).startswith(("http://", "https://")):
        return download_to_tempfile(bucket, bucket_path, tmp_dir)

    remote_url = remote_url or bucket_path
    if not remote_url:
        return None

    response = requests.get(remote_url, timeout=60)
    response.raise_for_status()

    resolved_name = filename or os.path.basename(remote_url) or str(uuid.uuid4())
    local_path = os.path.join(tmp_dir, resolved_name)
    with open(local_path, "wb") as handle:
        handle.write(response.content)
    return local_path



def build_ffmpeg_command(raw_video_path, final_render_path, *, audio_path=None, text_overlay_path=None):
    filter_chain = (
        f"[0:v]setpts={SLOW_MO_FACTOR}*PTS,"
        f"minterpolate=fps={SLOW_MO_OUTPUT_FPS}:mi_mode={SLOW_MO_INTERPOLATION_MODE}"
        "[bg]"
    )
    map_args = ["-map", "[bg]"]
    input_args = ["-i", raw_video_path]

    if audio_path:
        input_args.extend(["-i", audio_path])
        map_args = ["-map", "[v]", "-map", "1:a"]

    if text_overlay_path:
        input_args.extend(["-i", text_overlay_path])
        overlay_input_index = 2 if audio_path else 1
        filter_chain = (
            f"{filter_chain};"
            f"[bg][{overlay_input_index}:v]overlay=x=(W-w)/2:y=(H-h)/2[v]"
        )
        if not audio_path:
            map_args = ["-map", "[v]"]

    command = [
        "ffmpeg", "-y",
        *input_args,
        "-filter_complex", filter_chain,
        *map_args,
        "-pix_fmt", "yuv420p",
        "-r", str(SLOW_MO_OUTPUT_FPS),
        "-c:v", "libx264",
        "-crf", "18",
    ]

    if audio_path:
        command.extend(["-c:a", "aac", "-shortest"])

    command.append(final_render_path)
    return command



def process_slowmo_final(bucket, root_id, chunk_path, *, audio_path=None, text_overlay_path=None):
    with tempfile.TemporaryDirectory() as tmp:
        local_video_path = download_to_tempfile(bucket, chunk_path, tmp)
        local_audio_path = download_optional_asset(
            bucket,
            tmp,
            bucket_path=audio_path,
            remote_url=audio_path if audio_path and audio_path.startswith("http") else None,
            filename="audio_track",
        ) if audio_path else None
        local_text_overlay_path = download_optional_asset(
            bucket,
            tmp,
            bucket_path=text_overlay_path,
            remote_url=text_overlay_path if text_overlay_path and text_overlay_path.startswith("http") else None,
            filename="text_overlay.png",
        ) if text_overlay_path else None

        final_render_path = os.path.join(tmp, "final.mp4")
        ffmpeg_command = build_ffmpeg_command(
            local_video_path,
            final_render_path,
            audio_path=local_audio_path,
            text_overlay_path=local_text_overlay_path,
        )

        subprocess.run(ffmpeg_command, check=True)

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
        "source_image_url": image_url
    }

    for optional_field in (
        "audio_path",
        "audio_url",
        "text_overlay_path",
        "text_overlay_url",
    ):
        if data.get(optional_field):
            job[optional_field] = data[optional_field]

    bucket.blob(f"jobs/{root_id}.json").upload_from_string(json.dumps(job))

    payload = {
        "input": {
            "image_url": image_url,
            "steps": 20,
            "prompt": SVD_PROMPT,
            "width": 768,
            "height": 1344,
            "motion_bucket_id": 127,  # Controls motion amount; lower is usually more stable
            "cond_aug": 0.02,
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

        chunk_path = f"videos/{root_id}/chunk_0.mp4"
        bucket.blob(chunk_path).upload_from_string(video_bytes, content_type="video/mp4")

        job["status"] = "FINALIZING"
        job_blob.upload_from_string(json.dumps(job))

        final_url = process_slowmo_final(
            bucket,
            root_id,
            chunk_path,
            audio_path=job.get("audio_path") or job.get("audio_url"),
            text_overlay_path=job.get("text_overlay_path") or job.get("text_overlay_url"),
        )

        job["status"] = "COMPLETE"
        job["final_video_url"] = final_url
        job_blob.upload_from_string(json.dumps(job))

        return {
            "status": "COMPLETE",
            "final_video_url": final_url
        }, 200


    # ---- INITIAL BASE VIDEO REQUEST
    if "image_url" in data:
        return start_svd_base_video(data, bucket)

    return {
        "error": "Invalid payload",
        "received_content_type": request.content_type,
        "received_data": data,
        "hint": "Expected 'image_url' for new job or 'status'='COMPLETED' for callback"
    }, 400

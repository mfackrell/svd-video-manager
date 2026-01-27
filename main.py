//svd-video-manager main.py

import os, requests, uuid, base64, json, tempfile, subprocess
from functions_framework import http
from google.cloud import storage

CHUNK_FRAMES = 36
TOTAL_LOOPS = 4
VIDEO_BUCKET = "ssm-video-engine-output"
SVD_ENDPOINT_ID = os.environ["SVD_ENDPOINT_ID"]
SELF_URL = "https://svd-video-manager-710616455963.us-central1.run.app"

def extract_last_frame_png(video_bytes):
    with tempfile.TemporaryDirectory() as tmp:
        in_path, out_path = os.path.join(tmp, "chunk.mp4"), os.path.join(tmp, "last.png")
        with open(in_path, "wb") as f: f.write(video_bytes)
        subprocess.run(["ffmpeg", "-y", "-sseof", "-1", "-i", in_path, "-frames:v", "1", out_path], check=True)
        with open(out_path, "rb") as f: return f.read()

@http
def svd_video_manager(request):
    data = request.get_json(silent=True) or {}
    client = storage.Client()
    bucket = client.bucket(VIDEO_BUCKET)

    # WEBHOOK FROM RUNPOD
    if data.get("status") == "COMPLETED":
        root_id = request.args.get("root_id")
        job_blob = bucket.blob(f"jobs/{root_id}.json")
        job = json.loads(job_blob.download_as_text())
        
        video_bytes = base64.b64decode(data["output"]["video"].split(",")[-1])
        
        # Save Chunk
        loop = job["loop"]
        chunk_path = f"videos/{root_id}/chunk_{loop}.mp4"
        bucket.blob(chunk_path).upload_from_string(video_bytes, content_type="video/mp4")
        job["chunks"].append(chunk_path)

        # Advance State: Extract last frame for next loop
        last_frame_bytes = extract_last_frame_png(video_bytes)
        frame_path = f"images/{root_id}/last_frame_{loop}.png"
        bucket.blob(frame_path).upload_from_string(last_frame_bytes, content_type="image/png")
        job["current_image_url"] = f"https://storage.googleapis.com/{VIDEO_BUCKET}/{frame_path}"
        job["loop"] += 1

        if job["loop"] >= TOTAL_LOOPS:
            job["status"] = "COMPLETE"
            job_blob.upload_from_string(json.dumps(job))
            return {"status": "finished"}, 200

        # Submit next loop
        payload = {"input": {"image_url": job["current_image_url"], "steps": 10}, "webhook": f"{SELF_URL}?root_id={root_id}"}
        requests.post(f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run", headers={"Authorization": f"Bearer {os.environ['RUNPOD_API_KEY']}"}, json=payload)
        job_blob.upload_from_string(json.dumps(job))
        return {"status": "looping"}, 200

    # INITIAL TRIGGER
    image_url = data.get("image_url")
    root_id = uuid.uuid4().hex
    job = {"status": "PENDING", "root_id": root_id, "current_image_url": image_url, "loop": 0, "chunks": []}
    bucket.blob(f"jobs/{root_id}.json").upload_from_string(json.dumps(job))
    
    payload = {"input": {"image_url": image_url, "steps": 10}, "webhook": f"{SELF_URL}?root_id={root_id}"}
    requests.post(f"https://api.runpod.ai/v2/{SVD_ENDPOINT_ID}/run", headers={"Authorization": f"Bearer {os.environ['RUNPOD_API_KEY']}"}, json=payload)
    return {"state": "PENDING", "jobId": root_id}, 202

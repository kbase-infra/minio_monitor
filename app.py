# MinIO Metrics Viewer
# A simple web app to view storage metrics over time
import os
import csv
import io
from pathlib import Path
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import minio
import uvicorn

app = FastAPI(title="MinIO Metrics Viewer")

# Serve static files
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

required_config = ['MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD', "MINIO_ENDPOINT_URL"]

for var in required_config:
    if var not in os.environ:
        raise EnvironmentError(f"Required environment variable {var} not set.")


def get_s3_client():
    return minio.Minio(
        endpoint=os.environ["MINIO_ENDPOINT_URL"].replace("http://", "").replace("https://", ""),
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=os.environ["MINIO_ENDPOINT_URL"].startswith("https://"),
    )


def get_metrics_files(limit: int = 5):
    """Get the most recent metrics files from MinIO."""
    s3 = get_s3_client()
    bucket_name = "cdm-lake"
    metrics_prefix = "metrics/"

    metrics_files = []
    for obj in s3.list_objects(bucket_name, prefix=metrics_prefix):
        if obj.object_name.endswith('.csv'):
            metrics_files.append({
                'name': obj.object_name,
                'last_modified': obj.last_modified.isoformat(),
                'size': obj.size
            })

    metrics_files.sort(key=lambda x: x['last_modified'], reverse=True)
    return metrics_files[:limit]


def read_csv_from_minio(object_name: str):
    """Read and parse a CSV file from MinIO."""
    s3 = get_s3_client()
    bucket_name = "cdm-lake"

    response = s3.get_object(bucket_name, object_name)
    csv_content = response.read().decode('utf-8')
    response.close()
    response.release_conn()

    reader = csv.DictReader(io.StringIO(csv_content))
    return list(reader)


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = static_dir / "index.html"
    return HTMLResponse(content=html_path.read_text())


@app.get("/api/csv")
async def get_csv(file: str = Query(..., description="CSV filename to load")):
    if not file:
        raise HTTPException(status_code=400, detail="No file specified")

    try:
        rows = read_csv_from_minio(file)
        return {"rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/files")
async def list_files(limit: int = Query(5, ge=1, le=20)):
    files = get_metrics_files(limit=limit)
    return {"files": files}


if __name__ == '__main__':
    ROOT_PATH = os.environ.get("ROOT_PATH", "/minio-monitor")
    root_app = FastAPI()
    root_app.mount(ROOT_PATH, app)
    uvicorn.run(root_app, host='0.0.0.0', port=8000)
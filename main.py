# This script will connect to minio, get a list of buckets and their sizes.
# For each specified bucket, it will also list the paths and their sizes.
# It then checks for directories over quota and sends a summary to Slack.
import os
import csv
import io
import time
from datetime import datetime
import minio
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Start timing
start_time = time.time()

required_config = ['MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD', "MINIO_ENDPOINT_URL", "SLACK_BOT_TOKEN"]

for var in required_config:
    if var not in os.environ:
        raise EnvironmentError(f"Required environment variable {var} not set.")


def format_size(size_bytes):
    """Format size in appropriate units (MB or GB)."""
    size_gb = size_bytes / (1024 ** 3)
    if size_gb >= 1:
        return f"{size_gb:.2f} GB"
    else:
        size_mb = size_bytes / (1024 ** 2)
        return f"{size_mb:.2f} MB"


s3 = minio.Minio(
    endpoint=os.environ["MINIO_ENDPOINT_URL"].replace("http://", "").replace("https://", ""),
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=os.environ["MINIO_ENDPOINT_URL"].startswith("https://"),
)

# Collect bucket data
bucket_data = []
for bucket in s3.list_buckets():
    bucket_size = 0
    for obj in s3.list_objects(bucket.name, recursive=True):
        bucket_size += obj.size
    bucket_data.append({
        'path': bucket.name,
        'size_bytes': bucket_size,
        'size_human': format_size(bucket_size)
    })
    print(f"Bucket: {bucket.name}, Size: {format_size(bucket_size)}")

# Now get all directories and their sizes in the cdm-lake bucket (2 levels deep)
bucket_name = "cdm-lake"

# Collect all 2-level paths
two_level_paths = set()
for obj in s3.list_objects(bucket_name, recursive=True):
    parts = obj.object_name.split('/')
    if len(parts) >= 2:
        two_level_path = f"{parts[0]}/{parts[1]}"
    else:
        two_level_path = parts[0]
    two_level_paths.add(two_level_path)

# Calculate sizes for each 2-level path
path_sizes = {path: 0 for path in two_level_paths}
for obj in s3.list_objects(bucket_name, recursive=True):
    parts = obj.object_name.split('/')
    if len(parts) >= 2:
        two_level_path = f"{parts[0]}/{parts[1]}"
    else:
        two_level_path = parts[0]

    if two_level_path in path_sizes:
        path_sizes[two_level_path] += obj.size

# Collect path data and print human-readable output
path_data = []
print(f"\n{'=' * 60}")
print(f"Directory sizes in {bucket_name} (2 levels deep):")
print(f"{'=' * 60}")

for path, size in sorted(path_sizes.items()):
    full_path = f"{bucket_name}/{path}"
    path_data.append({
        'path': full_path,
        'size_bytes': size,
        'size_mb': size / (1024 ** 2),
        'size_gb': size / (1024 ** 3),
        'size_human': format_size(size)
    })
    print(f"{full_path}, Size: {format_size(size)}")

# Write CSV to memory buffer
csv_buffer = io.StringIO()
fieldnames = ['path', 'size_bytes', 'size_mb', 'size_gb', 'size_human']
writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

writer.writeheader()

# Write bucket summary
for bucket in bucket_data:
    writer.writerow({
        'path': bucket['path'],
        'size_bytes': bucket['size_bytes'],
        'size_mb': bucket['size_bytes'] / (1024 ** 2),
        'size_gb': bucket['size_bytes'] / (1024 ** 3),
        'size_human': bucket['size_human']
    })

# Write path details
for path in path_data:
    writer.writerow(path)

# Calculate runtime
end_time = time.time()
runtime_seconds = int(end_time - start_time)

# Save locally
csv_filename = "minio_sizes.csv"
with open(csv_filename, 'w', newline='') as f:
    f.write(csv_buffer.getvalue())
print(f"\n{'=' * 60}")
print(f"CSV exported locally to: {csv_filename}")

# Upload to MinIO with runtime in filename
datestamp = datetime.now().strftime("%Y-%m-%d")
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
minio_path = f"metrics/{datestamp}_{runtime_seconds}s.csv"

csv_bytes = csv_buffer.getvalue().encode('utf-8')
csv_bytes_io = io.BytesIO(csv_bytes)

s3.put_object(
    bucket_name=bucket_name,
    object_name=minio_path,
    data=csv_bytes_io,
    length=len(csv_bytes),
    content_type='text/csv'
)

print(f"CSV uploaded to: {bucket_name}/{minio_path}")

# Check for directories over quota
QUOTA_GB = 250
over_quota = []

for path in path_data:
    # Skip base bucket directories
    if '/' not in path['path']:
        continue

    if path['size_gb'] > QUOTA_GB:
        over_quota.append({
            'path': path['path'],
            'size_gb': path['size_gb'],
            'size_human': path['size_human']
        })

# Print quota results
print(f"\n{'=' * 60}")
print(f"Quota check: {QUOTA_GB} GB")
print(f"{'=' * 60}")

if over_quota:
    print(f"\n⚠️  {len(over_quota)} directory(s) over quota:\n")
    for item in sorted(over_quota, key=lambda x: x['size_gb'], reverse=True):
        overage = item['size_gb'] - QUOTA_GB
        print(f"  {item['path']}")
        print(f"    Size: {item['size_human']} ({overage:.2f} GB over quota)")
        print()
else:
    print("\n✅ All directories are within quota.")

# Send summary to Slack
client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
channel_name = "berdl_minio_notifications"

try:
    message = f"*MinIO Storage Report* - {timestamp}\n"
    message += f"_Runtime: {runtime_seconds} seconds_\n\n"

    if over_quota:
        message += f":warning: *{len(over_quota)} directory(s) over quota ({QUOTA_GB} GB):*\n"
        for item in sorted(over_quota, key=lambda x: x['size_gb'], reverse=True):
            overage = item['size_gb'] - QUOTA_GB
            message += f"• `{item['path']}` - {item['size_human']} ({overage:.2f} GB over)\n"
    else:
        message += f":white_check_mark: All directories are within the quota of {QUOTA_GB} GB."

    message += f"\n\n_Metrics saved to `{bucket_name}/{minio_path}`_"

    response = client.chat_postMessage(
        channel=channel_name,
        text=message
    )
    print(f"\nMessage posted to Slack channel {channel_name}")
except SlackApiError as e:
    print(f"Error posting to Slack: {e.response['error']}")

print(f"\nTotal runtime: {runtime_seconds} seconds")
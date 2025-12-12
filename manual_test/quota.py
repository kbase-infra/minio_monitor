# This script checks the latest metrics file for directories over 100GB
import os
import csv
import io
import minio

required_config = ['MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD', "MINIO_ENDPOINT_URL", "SLACK_BOT_TOKEN"]

for var in required_config:
    if var not in os.environ:
        raise EnvironmentError(f"Required environment variable {var} not set.")

s3 = minio.Minio(
    endpoint=os.environ["MINIO_ENDPOINT_URL"].replace("http://", "").replace("https://", ""),
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=os.environ["MINIO_ENDPOINT_URL"].startswith("https://"),
)

bucket_name = "cdm-lake"
metrics_prefix = "metrics/"

# Get all CSV files in the metrics directory
metrics_files = []
for obj in s3.list_objects(bucket_name, prefix=metrics_prefix):
    if obj.object_name.endswith('.csv'):
        metrics_files.append(obj)

if not metrics_files:
    print("No metrics files found.")
    exit(1)

# Sort by last modified and get the latest
latest_file = max(metrics_files, key=lambda x: x.last_modified)
print(f"Latest metrics file: {latest_file.object_name}")
print(f"Last modified: {latest_file.last_modified}")

# Download and parse the CSV
response = s3.get_object(bucket_name, latest_file.object_name)
csv_content = response.read().decode('utf-8')
response.close()
response.release_conn()

# Parse CSV
reader = csv.DictReader(io.StringIO(csv_content))

QUOTA_GB = 250
over_quota = []

for row in reader:
    path = row['path']

    # Skip base bucket directories
    if '/' not in path:
        continue

    size_gb = float(row['size_gb'])
    if size_gb > QUOTA_GB:
        over_quota.append({
            'path': path,
            'size_gb': size_gb,
            'size_human': row['size_human']
        })

# Print results
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




from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
channel_name = "berdl_minio_notifications"

# send a summary to slack
try:
    if over_quota:
        message = f":warning: *{len(over_quota)} directory(s) over quota ({QUOTA_GB} GB):*\n"
        for item in sorted(over_quota, key=lambda x: x['size_gb'], reverse=True):
            overage = item['size_gb'] - QUOTA_GB
            message += f"• `{item['path']}` - {item['size_human']} ({overage:.2f} GB over quota)\n"
    else:
        message = f":white_check_mark: All directories are within the quota of {QUOTA_GB} GB."

    response = client.chat_postMessage(
        channel=channel_name,
        text=message
    )
    print(f"Message posted to Slack channel {channel_name}")
except SlackApiError as e:
    print(f"Error posting to Slack: {e.response['error']}")

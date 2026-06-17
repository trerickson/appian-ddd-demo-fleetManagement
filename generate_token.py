"""
Generates a signed token for Appian's connected system to send as the
X-API-Key header on every request to this API.

Usage: python generate_token.py <INBOUND_API_SECRET> [days_valid]

INBOUND_API_SECRET must match the env var configured on the running container.
days_valid defaults to 30.
"""
import sys
import jwt
from datetime import datetime, timedelta

secret = sys.argv[1]
days_valid = int(sys.argv[2]) if len(sys.argv) > 2 else 30

token = jwt.encode(
    {"exp": datetime.utcnow() + timedelta(days=days_valid)},
    secret,
    algorithm="HS256",
)

print(token)
print(f"\nExpires in {days_valid} days. Set this as the X-API-Key header value in Appian's connected system.", file=sys.stderr)

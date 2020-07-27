import base64

from google.cloud import kms_v1
from google.cloud import secretmanager


def decrypt_secret(project, region, keyring, key, secret_base64):
    secret_enc = base64.b64decode(secret_base64)
    kms_client = kms_v1.KeyManagementServiceClient()
    key_path = kms_client.crypto_key_path_path(project, region, keyring, key)
    secret = kms_client.decrypt(key_path, secret_enc)
    return secret.plaintext.decode("utf-8").replace('\n', '')


def get_secret(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode('UTF-8')
    return payload

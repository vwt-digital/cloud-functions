import base64
from google.cloud import kms_v1


def decrypt_secret(project, region, keyring, key, secret_base64):
    secret_enc = base64.b64decode(secret_base64)
    kms_client = kms_v1.KeyManagementServiceClient()
    key_path = kms_client.crypto_key_path_path(project, region, keyring, key)
    secret = kms_client.decrypt(key_path, secret_enc)
    return secret.plaintext.decode("utf-8").replace('\n', '')

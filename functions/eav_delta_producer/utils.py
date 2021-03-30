from google.cloud import secretmanager_v1


def get_secret(project_id, secret_id):
    client = secretmanager_v1.SecretManagerServiceClient()

    secret_name = client.secret_version_path(project_id, secret_id, "latest")

    response = client.access_secret_version(request={"name": secret_name})
    payload = response.payload.data.decode("UTF-8")

    return payload

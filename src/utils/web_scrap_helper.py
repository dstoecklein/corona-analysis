import io
import requests


def http_request(url: str, decode: bool = True):
    response = requests.get(url)
    if decode:
        try:
            return io.StringIO(response.content.decode('utf-8'))
        except UnicodeDecodeError:
            return io.StringIO(response.content.decode('ISO-8859-1'))
    else:
        return response.content

import requests


headers: dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/86.0.4240.111 Safari/537.36'
        }

class GeoLocation():
    def __init__(self) -> None:
        pass

    def get_location(self):
        response = requests.get(f'https://ipapi.co/json/', headers=headers).json()
        return response


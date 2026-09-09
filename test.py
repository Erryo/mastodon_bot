from mastodon import Mastodon
import os

from requests import api

try:
    Client_ID = os.environ["MASTODONID"]
    Client_Secret = os.environ["MASTODONSECRET"]
    Client_Token = os.environ["MASTODONTOKEN"]
except KeyError:
    print("Environment variables not set")
    os._exit(-1)

mastodon = Mastodon(
    client_id=Client_ID,
    client_secret=Client_Secret,
    access_token=Client_Token,
    api_base_url="https://mastodon.social",
)
print(mastodon.auth_request_url())


# To post, create an actual API instance:
# mastodon = Mastodon(access_token="pytooter_usercred.secret")
mastodon.toot("Tooting from Python using #mastodonpy !")

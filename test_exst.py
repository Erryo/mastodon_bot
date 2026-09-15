import os
from mastodon import Mastodon

from dotenv import load_dotenv, dotenv_values

load_dotenv()


def main():
    try:
        client_id = os.environ["STREAMID"]
        client_secret = os.environ["STREAMSECRET"]
        client_token = os.environ["STREAMTOKEN"]
        local_url = os.environ["STREAMURL"]
    except KeyError as e:
        print(f"Environment variable not set: {e}")
        return

    mastodon = Mastodon(
        client_id=client_id,
        client_secret=client_secret,
        access_token=client_token,
        api_base_url=local_url,
        ratelimit_method="wait",
    )

    status_id = input("Status ID: ").strip()

    try:
        status = mastodon.status(status_id)
        print("Status exists.")
        print(f"Author: @{status['account']['acct']}")
        print(f"Content: {status['content']}")
    except Exception as e:
        print(f"Status does not exist or could not be retrieved: {e}")


if __name__ == "__main__":
    main()

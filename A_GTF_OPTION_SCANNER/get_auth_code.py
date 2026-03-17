from fyers_apiv3 import fyersModel
import webbrowser

CLIENT_ID = "paste your client id"
SECRET_KEY = "paste your secret key"
REDIRECT_URI = "http://127.0.0.1/"

session = fyersModel.SessionModel(
    client_id=CLIENT_ID,
    secret_key=SECRET_KEY,
    redirect_uri=REDIRECT_URI,
    response_type="code",
    grant_type="authorization_code"
)

auth_url = session.generate_authcode()
print(auth_url)
webbrowser.open(auth_url)

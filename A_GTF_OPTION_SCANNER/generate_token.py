from fyers_apiv3 import fyersModel

CLIENT_ID = "paste your client id"
SECRET_KEY = "paste your secret key"
REDIRECT_URI = "http://127.0.0.1/"

AUTH_CODE = "paste your auth code"

session = fyersModel.SessionModel(
    client_id=CLIENT_ID,
    secret_key=SECRET_KEY,
    redirect_uri=REDIRECT_URI,
    response_type="code",
    grant_type="authorization_code"
)

session.set_token(AUTH_CODE)
response = session.generate_token()
print(response)

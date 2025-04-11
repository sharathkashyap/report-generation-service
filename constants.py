import os

DEFAULT_TABLE_NAME = os.environ.get('DEFAULT_TABLE_NAME', 'wf_status')
USER_DETAILS_TABLE = os.environ.get('USER_DETAILS_TABLE', 'user_detail')
CONTENT_TABLE = os.environ.get('CONTENT_TABLE', 'content')
USER_ENROLMENTS_TABLE = os.environ.get('USER_ENROLMENTS_TABLE', 'user_enrolment')
USER_CONSUMPTION_TABLE = os.environ.get('USER_CONSUMPTION_TABLE', 'user_consumption')
REQUIRED_COLUMNS_FOR_ENROLLMENTS = ["user_id","mdo_name","content_id","content_name","content_category","batch_id","enrolled_on","user_consumption_status","content_progress_percentage","certificate_generated","certificate_id","completed_on","certificate_generated_on"]
SUNBIRD_SSO_URL = os.environ.get('SUNBIRD_SSO_URL', 'https://sso.example.com')
SUNBIRD_SSO_REALM = os.environ.get('SUNBIRD_SSO_REALM', 'https://sso.example.com')
ACCESS_TOKEN_PUBLICKEY_BASEPATH = os.environ.get('accesstoken_publickey_basepath')
IS_VALIDATION_ENABLED = os.environ.get('IS_VALIDATION_ENABLED', 'false')
X_AUTHENTICATED_USER_TOKEN = 'x-authenticated-user-token'
postgres_db_user = os.environ.get('postgres_db_user', 'postgres')
postgres_db_password = os.environ.get('postgres_db_password', 'password123')
postgres_db_host = os.environ.get('postgres_db_host', 'localhost')
postgres_db_port = os.environ.get('postgres_db_port', 5431)
postgres_db_name = os.environ.get('postgres_db_name', 'warehouse')
postgres_db_url = f"postgresql://{postgres_db_user}:{postgres_db_password}@{postgres_db_host}:{postgres_db_port}/{postgres_db_name}"
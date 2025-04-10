import os

DEFAULT_TABLE_NAME = os.environ.get('DEFAULT_TABLE_NAME', 'wf_status')
USER_DETAILS_TABLE = os.environ.get('USER_DETAILS_TABLE', 'user_detail')
CONTENT_TABLE = os.environ.get('CONTENT_TABLE', 'content')
USER_ENROLMENTS_TABLE = os.environ.get('USER_ENROLMENTS_TABLE', 'user_enrolments')
REQUIRED_COLUMNS_FOR_ENROLLMENTS = ["user_id", "full_name", "content_id","content_name","content_type","content_type","certificate_id","enrolled_on","certificate_generated","first_completed_on","last_completed_on","content_duration","content_progress_percentage"]
SUNBIRD_SSO_URL = os.environ.get('SUNBIRD_SSO_URL', 'https://sso.example.com')
SUNBIRD_SSO_REALM = os.environ.get('SUNBIRD_SSO_REALM', 'https://sso.example.com')
ACCESS_TOKEN_PUBLICKEY_BASEPATH = os.environ.get('accesstoken_publickey_basepath')
IS_VALIDATION_ENABLED = os.environ.get('IS_VALIDATION_ENABLED', 'false')
X_AUTHENTICATED_USER_TOKEN = 'x-authenticated-user-token'
postgres_db_user = os.environ.get('postgres_db_user', 'postgres')
postgres_db_password = os.environ.get('postgres_db_password', 'password')
postgres_db_host = os.environ.get('postgres_db_host', 'localhost')
postgres_db_port = os.environ.get('postgres_db_port', 5433)
postgres_db_name = os.environ.get('postgres_db_name', 'warehouse')
postgres_db_url = f"postgresql://{postgres_db_user}:{postgres_db_password}@{postgres_db_host}:{postgres_db_port}/{postgres_db_name}"